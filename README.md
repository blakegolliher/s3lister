# s3lister

A high-performance S3 object lister that records every object's key, size, and
last-modified time into **Parquet files you can query directly with DuckDB** —
no database, no import step. Designed for buckets with tens of millions to
billions of objects.

## Features

- **Fast**: work-stealing readers fan out `ListObjectsV2` across the whole
  keyspace; per-page batching keeps channel and lock overhead near zero.
- **Parquet output**: one `part-NNN.parquet` file per writer, zstd-compressed,
  1,000,000-row row groups. Columnar and tiny on disk.
- **DuckDB-native**: query the output the instant a scan finishes —
  `SELECT * FROM 'out/*.parquet'`. Also works with Polars, PyArrow, Athena,
  Spark, ClickHouse.
- **Analytics-ready schema**: precomputed `object_name`, `extension`,
  `parent_prefix`, and `depth` columns give predicate pushdown for free.
- **Work-stealing parallelism**: idle readers steal work; huge flat prefixes are
  range-split so no single worker becomes a straggler.
- **Live progress bar** with throughput, queue depth, and elapsed time, plus a
  detailed stats block written to a log file.
- **Pure Go**: no CGO, no external services.

## Example

```
[bg@chewbacca s3lister]$ ./s3lister scan -config ./config.toml
⠹ [░░░░░░░░░█▓░░░░░░░░░░░░░░░░░]  385,061,524 objs  30,193/s  q:2,039  3h32m33s

Done! 385061524 objects in 3h32m36s
  avg 30185/s   peak 34011/s
  output: ./s3lister_out  (8 files, 6.2 GiB)
  log: ./s3lister.log

Query it with DuckDB:
  duckdb -c "SELECT count(*), sum(size_bytes) FROM './s3lister_out/*.parquet'"
```

## Quick Start

```bash
# Build
go build -o s3lister .

# Configure credentials
cp config.toml.example config.toml
vi config.toml

# Scan a bucket -> writes ./s3lister_out/part-*.parquet
./s3lister scan -config config.toml

# Query it immediately with DuckDB (no import step)
duckdb -c "SELECT count(*), sum(size_bytes) FROM 's3lister_out/*.parquet'"
```

## Commands

### `scan`

Connects to S3, lists all objects under the configured bucket/prefix, and writes
the results as Parquet part files into the output directory.

```
./s3lister scan [options]

Options:
  -config string   Path to config file (default "config.toml")
  -readers int     Override number of reader threads
  -writers int     Override number of writer threads
  -verbose         Log to stderr and trace HTTP requests
```

Progress is shown as a live bar on the terminal; a status line is also written
to the log file every 5 seconds. Each writer produces one `part-NNN.parquet`
file, so `-writers 8` yields `part-000.parquet` … `part-007.parquet`.

Each run starts fresh: existing `part-*.parquet` files in the output directory
are removed before the scan begins.

### `export-csv`

Streams the Parquet dataset into a single CSV file. Optional — most users query
the Parquet directly with DuckDB — but handy for tools that only speak CSV.

```
./s3lister export-csv [options]

Options:
  -in string    Path to the Parquet output directory (default "./s3lister_out")
  -out string   Output CSV file path (default "s3objects.csv")
```

Columns: `key`, `size_bytes`, `last_modified`, `storage_class`, `etag`.

## Querying with DuckDB

The output is a set of Parquet files, so DuckDB queries it as one table with a
glob. No load step:

```sql
-- Overview
SELECT count(*), round(sum(size_bytes)/1e9, 2) AS total_gb
FROM 's3lister_out/*.parquet';

-- Bytes by extension
SELECT extension, count(*), round(sum(size_bytes)/1e9, 2) AS gb
FROM 's3lister_out/*.parquet'
GROUP BY extension ORDER BY gb DESC LIMIT 20;

-- Largest objects
SELECT key, size_bytes
FROM 's3lister_out/*.parquet'
ORDER BY size_bytes DESC LIMIT 20;
```

See **[docs/QUERY_DUCKDB.md](docs/QUERY_DUCKDB.md)** for the full schema and a
library of ready-to-run queries (fullest prefixes, storage-class breakdown, size
histograms, duplicate detection by ETag, cross-scan diffs, and more).

### Schema

| Column | Type | Notes |
|--------|------|-------|
| `key` | VARCHAR | Full object key |
| `object_name` | VARCHAR | Basename after the last `/` |
| `extension` | VARCHAR | Text after the last `.` (`""` if none) |
| `parent_prefix` | VARCHAR | Everything before the last `/` |
| `depth` | INTEGER | Count of `/` separators |
| `size_bytes` | BIGINT | Object size |
| `last_modified` | TIMESTAMP | Last-modified time (UTC) |
| `etag` | VARCHAR | ETag, quotes stripped |
| `storage_class` | VARCHAR | e.g. `STANDARD`, `GLACIER` |
| `scan_id` | VARCHAR | Identifier of the scan run |
| `scan_timestamp` | TIMESTAMP | When the scan started (UTC) |

## Configuration

```toml
[s3]
access_key = "YOUR_ACCESS_KEY"
secret_key = "YOUR_SECRET_KEY"
endpoint   = "https://s3.amazonaws.com"
bucket     = "my-bucket"
prefix     = ""              # empty = whole bucket
region     = "us-east-1"

[workers]
readers    = 32              # goroutines listing S3 (ListObjectsV2)
writers    = 8              # goroutines writing Parquet (one file each)
queue_size = 100000         # records buffered between readers and writers

[storage]
output_dir = "./s3lister_out"   # receives the part-NNN.parquet files

[logging]
log_file = "./s3lister.log"
```

`-readers` / `-writers` on the command line override the config values.

## Benchmarking

Benchmark tooling lives in a separate binary, `s3lister-bench`, so the main
tool stays focused on scanning. See **[bench-readme.md](bench-readme.md)** for
how we generate benchmark buckets (5M / 100M / 2B keys), how we measure, and
how to reproduce the numbers yourself.

## Performance

Tuning notes:

- **Readers** are network-bound. On high-latency or high-object-count buckets,
  more readers means more in-flight `ListObjectsV2` requests. 32–128 is typical;
  the work-stealing scheduler keeps them all busy.
- **Point the endpoint at a DNS name, not one VIP.** New connections are
  rotated across every IP the name resolves to (re-resolved every 30s), so
  scale-out front ends see even load across all their nodes. The log shows
  the discovered addresses at startup.
- **Writers** are CPU-bound on zstd compression. Roughly one writer per core is a
  good starting point. More writers = more output part files.
- **Large flat prefixes** (millions of keys under one prefix with no delimiters)
  are automatically range-split into parallel chunks so a single prefix does not
  bottleneck one reader.

### Why So Fast?

1. **Work-stealing readers** — every reader stays busy; discovered sub-prefixes
   and range chunks are stolen by idle workers.
2. **Per-page batching** — readers hand off a whole `ListObjectsV2` page in one
   channel send and one atomic update, not one per object.
3. **One Parquet file per writer** — writers never share a handle or a lock, so
   write throughput scales with cores.
4. **No LSM, no compaction** — unlike a key-value store, Parquet writes each row
   group once. There is no background rewrite of data you never update.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        main.go                          │
│                   (orchestration)                        │
└──────────┬────────────────────────────────┬──────────────┘
           │                                │
           ▼                                ▼
┌─────────────────────┐   []ObjectRecord ┌─────────────────────┐
│    ReaderPool        │   (per S3 page)  │    WriterPool        │
│  (N goroutines)      │─────────────────▶│  (M goroutines)      │
│  work-stealing deques│   buffered chan  │  one .parquet each   │
└──────────┬───────────┘                  └──────────┬───────────┘
           ▼                                          ▼
┌─────────────────────┐                  ┌─────────────────────┐
│   S3 ListObjectsV2   │                  │  part-000.parquet    │
│                      │                  │  part-001.parquet    │
│                      │                  │  ...  (zstd, columnar)│
└──────────────────────┘                  └─────────────────────┘
                                                     │
                                                     ▼
                                            ┌─────────────────┐
                                            │  DuckDB / Polars │
                                            │  Athena / Spark  │
                                            └─────────────────┘
```

See **[ARCHITECTURE.md](ARCHITECTURE.md)** for the full design: prefix
discovery, work-stealing deques, dynamic range-splitting, and the Parquet
writer.

## Documentation

- [Architecture & Build Guide](ARCHITECTURE.md) — internals and build steps
- [DuckDB Queries](docs/QUERY_DUCKDB.md) — schema and query library

## License

MIT
