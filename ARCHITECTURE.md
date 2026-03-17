# Architecture & Build Guide

## Building

```bash
# Clone and build
git clone <repo>
cd s3lister
go build -o s3lister .

# Or install directly
go install .
```

No CGO required. All dependencies are pure Go, including the Pebble database engine (CockroachDB's RocksDB replacement).

### Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/cockroachdb/pebble` | Embedded key-value store (pure Go) |
| `github.com/aws/aws-sdk-go-v2` | S3 API client |
| `github.com/BurntSushi/toml` | Config file parsing |

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                        main.go                          │
│                   (orchestration)                        │
└──────────┬────────────────────────────────┬──────────────┘
           │                                │
           ▼                                ▼
┌─────────────────────┐          ┌─────────────────────┐
│    ReaderPool        │          │    WriterPool        │
│  (N goroutines)      │          │  (M goroutines)      │
│                      │          │                      │
│  ┌────┐ ┌────┐      │  chan    │  batch 5000 records  │
│  │ D1 │ │ D2 │ ...  │────────▶│  per Pebble commit   │
│  └────┘ └────┘      │          │                      │
│   work-stealing      │          │                      │
│   deques             │          │                      │
└──────────┬───────────┘          └──────────┬───────────┘
           │                                 │
           ▼                                 ▼
┌─────────────────────┐          ┌─────────────────────┐
│    S3 API            │          │    Pebble DB         │
│  (ListObjectsV2)     │          │  (key → 16 bytes)    │
└──────────────────────┘          └──────────────────────┘
```

The system is a two-stage pipeline connected by a buffered Go channel.

## Stage 1: Reader Pool (S3 Listing)

**Package:** `internal/worker/reader.go`, `internal/worker/deque.go`

### Prefix Discovery

On startup, readers do a delimiter-based listing (`/`) to find top-level "directories" in the bucket. These become independent work units that can be listed in parallel.

If fewer prefixes are found than workers, a second pass expands each prefix into its sub-prefixes so every worker starts with something to do.

### Work-Stealing Deques

Each reader goroutine owns a **double-ended queue** (deque) implemented as a ring buffer (`deque.go`). The deque supports O(1) operations on both ends:

- **Owner** pushes/pops from the **front** (LIFO — process most recently discovered prefixes first for cache locality)
- **Thieves** steal from the **back** (FIFO — take the oldest, typically largest, work chunks)

When a thief steals, it takes **half** the victim's deque. This amortizes the cost of stealing and redistributes work evenly.

### Dynamic Splitting

When a reader picks up a work item, it first tries to **split** the prefix into sub-prefixes via a delimiter listing. If sub-prefixes exist, they get pushed onto the local deque where other idle workers can immediately steal them. This creates a recursive fan-out:

```
"data/" → split → ["data/2023/", "data/2024/"]
                      │
                      ▼ (stolen by idle worker)
              "data/2023/" → split → ["data/2023/01/", "data/2023/02/", ...]
```

Splitting stops at depth 4 to prevent excessive API calls on deeply nested structures.

### Idle Behavior

When a worker's deque is empty:

1. Try to steal from a random peer
2. If all peers are empty, enter a spin-wait with **exponential backoff** (500μs → 50ms)
3. Give up after 3 seconds if no work appears and no other workers are active

## Stage 2: Writer Pool (Pebble Persistence)

**Package:** `internal/worker/writer.go`, `internal/store/store.go`

Writer goroutines consume `ObjectRecord` structs from the shared channel and batch them into groups of 5000 before committing to Pebble.

### Storage Format

Each record is stored as:

- **Key:** The S3 object key (string bytes)
- **Value:** 16 bytes — `[8 bytes: size as uint64 LE][8 bytes: last_modified as unix nanos LE]`

### Pebble Tuning

The database is configured for write-heavy throughput:

| Setting | Value | Why |
|---------|-------|-----|
| MemTableSize | 128 MB | Large buffer before flushing to SST |
| MemTableStopWritesThreshold | 4 | Allow 4 memtables before stalling |
| MaxConcurrentCompactions | 4 | Parallel background compaction |
| L0CompactionThreshold | 8 | Tolerate L0 buildup before compacting |
| L0StopWritesThreshold | 36 | High ceiling before write stalls |
| DisableWAL | true | No crash recovery needed (re-scan S3) |
| NoSync on commit | true | OS can buffer writes |

## Connecting the Pipeline

The buffered channel (`queue_size` in config) decouples readers from writers:

- **Readers faster than writers:** Channel fills up, readers block on send, back-pressure naturally throttles S3 calls
- **Writers faster than readers:** Channel drains, writers block on receive, wake instantly when data arrives
- **Balanced:** Both sides stay busy, channel acts as a shock absorber for burst variance

This means the system is always either listing or writing (or both). No goroutine sits idle while there's work to do.

## S3 Client

**Package:** `internal/s3client/client.go`

The HTTP transport is tuned for high concurrency:

- 500 max idle connections (matches potential reader count)
- Keep-alive enabled
- Compression disabled (S3 list responses are small, avoid CPU overhead)
- 120s overall timeout

A `ListBuckets` call on startup validates credentials and connectivity before committing to a full scan.

## Export Path

The `export-csv` command opens Pebble in read-only mode and iterates all keys in sorted order. It uses:

- `encoding/csv` for correct RFC 4180 output
- 4 MB buffered writer for I/O efficiency
- 30-second progress reporting

## Project Layout

```
s3lister/
├── main.go                      # CLI, subcommands, orchestration
├── config.toml                  # Example configuration
├── internal/
│   ├── config/config.go         # TOML parsing and validation
│   ├── s3client/client.go       # S3 client construction
│   ├── store/store.go           # Pebble wrapper (read, write, iterate)
│   └── worker/
│       ├── reader.go            # Reader pool + work-stealing logic
│       ├── writer.go            # Writer pool + batching
│       └── deque.go             # Ring-buffer deque for work stealing
├── go.mod
└── go.sum
```
