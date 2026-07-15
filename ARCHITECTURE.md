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

No CGO required. All dependencies are pure Go, including the Parquet writer.

### Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/parquet-go/parquet-go` | Parquet reader/writer (pure Go) |
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
│                      │  chan    │                      │
│  ┌────┐ ┌────┐      │ []Object │  one part-NNN.parquet│
│  │ D1 │ │ D2 │ ...  │─Record──▶│  per writer          │
│  └────┘ └────┘      │ (a page) │  (zstd, 1M-row groups)│
│   work-stealing      │          │                      │
│   deques             │          │                      │
└──────────┬───────────┘          └──────────┬───────────┘
           │                                 │
           ▼                                 ▼
┌─────────────────────┐          ┌─────────────────────┐
│    S3 API            │          │  Parquet files       │
│  (ListObjectsV2)     │          │  (DuckDB-queryable)  │
└──────────────────────┘          └──────────────────────┘
```

The system is a two-stage pipeline connected by a buffered Go channel. The
channel carries **one whole `ListObjectsV2` page per message** (a
`[]ObjectRecord` batch), so per-object channel and atomic overhead is amortized
across up to 1000 objects.

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

## Stage 2: Writer Pool (Parquet)

**Package:** `internal/worker/writer.go`, `internal/pq/pq.go`

Each writer goroutine owns exactly **one output file** — `part-000.parquet`,
`part-001.parquet`, and so on. Because no two writers share a file handle, there
is no write lock and throughput scales with cores until zstd or disk IO
saturates. DuckDB reads the whole set as a single table via a glob:

```sql
SELECT * FROM 's3lister_out/*.parquet';
```

Writers consume `[]ObjectRecord` batches from the channel, convert each record
into a Parquet `Row` (computing the derived columns), and append. When a row
group reaches 1,000,000 rows the writer flushes it and starts a new one, so
memory stays flat regardless of total object count.

### Schema

Each row is one S3 object. Derived columns are precomputed at write time so
DuckDB filters get predicate/statistics pushdown instead of parsing keys per row.

| Column | Parquet type | Source |
|--------|--------------|--------|
| `key` | UTF8 | S3 object key |
| `object_name` | UTF8 | basename after last `/` |
| `extension` | UTF8 | text after last `.` in the name |
| `parent_prefix` | UTF8 | everything before the last `/` |
| `depth` | INT32 | count of `/` in the key |
| `size_bytes` | INT64 | object size |
| `last_modified` | TIMESTAMP(MICROS, UTC) | object last-modified time |
| `etag` | UTF8 | ETag, surrounding quotes stripped |
| `storage_class` | UTF8 | e.g. `STANDARD`, `GLACIER` |
| `scan_id` | UTF8 | identifier for the scan run |
| `scan_timestamp` | TIMESTAMP(MICROS, UTC) | when the scan started |

`scan_id` and `scan_timestamp` are constant within a run, so they compress to
almost nothing; they let you union several scans into one dataset and filter or
diff by run.

### Parquet Tuning

| Setting | Value | Why |
|---------|-------|-----|
| Compression | zstd (level 3) | Best size/speed balance; S3 keys share prefixes and compress hard |
| MaxRowsPerRowGroup | 1,000,000 | Bounds writer memory; auto-flush on reach |
| Codec concurrency | 1 per writer | Parallelism comes from the writer pool, not per-codec threads |
| Output buffering | 4 MB `bufio` per file | Big sequential IO to disk |

### Why Parquet Instead of a Key-Value Store?

For a write-once bucket snapshot, an LSM key-value store (Pebble/RocksDB) pays
for features this workload never uses: it sorts and **rewrites data repeatedly**
during background compaction (write amplification), and its values are opaque
blobs that still need an export step before any query engine can read them.

Parquet writes each row group exactly once, is columnar (so aggregates scan only
the columns they touch), compresses far better on repetitive keys, and is read
natively by DuckDB, Polars, PyArrow, Athena, Spark, and ClickHouse — no export.

## Connecting the Pipeline

A buffered channel decouples readers from writers. It carries `[]ObjectRecord`
batches (one per S3 page), so `queue_size` in config — expressed in records — is
converted to a batch capacity (`queue_size / 1000`, floored so every reader has
slack). Back-pressure works the same either way:

- **Readers faster than writers:** Channel fills up, readers block on send, back-pressure naturally throttles S3 calls
- **Writers faster than readers:** Channel drains, writers block on receive, wake instantly when data arrives
- **Balanced:** Both sides stay busy, channel acts as a shock absorber for burst variance

This means the system is always either listing or writing (or both). No goroutine sits idle while there's work to do.

## Progress & Stats

**Package:** `internal/progress/progress.go`

A single goroutine drives a `progress.Meter` on a 100 ms ticker. On a terminal it
renders a live sweeping bar with humanized object count, smoothed (EMA) and peak
throughput, queue depth, and elapsed time; on a non-TTY it degrades to a status
line every 5 seconds. A 5-second ticker also writes a `[progress]` line to the
log file. On completion, a stats block (objects, avg/peak rate, output size,
bytes-per-object, error count) is written to both stderr and the log.

## S3 Client

**Package:** `internal/s3client/client.go`

The HTTP transport is tuned for high concurrency:

- 500 max idle connections (matches potential reader count)
- Keep-alive enabled
- Compression disabled (S3 list responses are small, avoid CPU overhead)
- 120s overall timeout

A `HeadBucket` call on startup validates credentials and connectivity before
committing to a full scan. (`HeadBucket` rather than `ListBuckets` so it works on
endpoints that only grant access to the one configured bucket.)

## Query & Export Path

The primary "query path" is external: DuckDB (or any Parquet reader) globs the
part files directly — see [docs/QUERY_DUCKDB.md](docs/QUERY_DUCKDB.md).

The optional `export-csv` command is for tools that only speak CSV. It opens each
`part-*.parquet` with a `parquet.GenericReader[pq.Row]`, streams rows through
`encoding/csv` into a 4 MB buffered writer, and reports progress every 30
seconds.

## Project Layout

```
s3lister/
├── main.go                      # CLI, subcommands, orchestration, stats
├── config.toml                  # Configuration (gitignored; copy from example)
├── internal/
│   ├── config/config.go         # TOML parsing and validation
│   ├── model/record.go          # Shared ObjectRecord type (no storage deps)
│   ├── s3client/client.go       # S3 client construction
│   ├── pq/pq.go                 # Parquet schema + per-file writer (zstd)
│   ├── progress/progress.go     # TTY-aware progress bar + rate stats
│   └── worker/
│       ├── reader.go            # Reader pool + work-stealing + range-split
│       ├── writer.go            # Writer pool (one parquet file per writer)
│       └── deque.go             # Ring-buffer deque for work stealing
├── docs/QUERY_DUCKDB.md         # Schema + DuckDB query library
├── go.mod
└── go.sum
```
