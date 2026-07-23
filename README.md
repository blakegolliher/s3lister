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
- **Object tags (opt-in)**: `scan -tags` fetches every object's tags into a
  queryable `tags` map column — `WHERE map_extract(tags, 'env') = ['prod']`
  in DuckDB.
- **Work-stealing parallelism**: idle readers steal work; huge flat prefixes are
  range-split so no single worker becomes a straggler.
- **Live progress bar** with throughput and elapsed time; queue depth and
  per-worker detail go to the log file, along with a final stats block.
- **Pure Go**: no CGO, no external services.

## Example

A real run against a bucket with two billion objects:

```
$ ./s3lister scan -config ./config.toml -bucket bench-2b -output ./out-2b -readers 64
⠴ 5,557,064 objs  412,166/s  15s

Done! 2000000000 objects in 1h20m33.261s
  avg 413799/s   peak 733128/s
  output: ./out-2b  (8 files, 17.0 GiB)
  log: ./s3lister.log

Query it with DuckDB:
  duckdb -c "SELECT count(*), sum(size_bytes) FROM './out-2b/*.parquet'"
```

## Download

Prebuilt binaries for Linux, macOS, and Windows are on the
**[Releases page](https://github.com/blakegolliher/s3lister/releases)** —
each archive contains `s3lister`, the `s3lister-bench` benchmarking tool, and
an example config. No runtime dependencies.

```bash
curl -LO https://github.com/blakegolliher/s3lister/releases/latest/download/s3lister_linux_amd64.tar.gz
tar xzf s3lister_linux_amd64.tar.gz
./s3lister version
```

Or build from source (Go 1.24+): clone the repo and run `make`.

## Quick Start

```bash
# 1. Get the binary — download a release (above), or clone and build:
make                  # -> ./s3lister

# 2. Configure: copy the example and fill in your endpoint + credentials
cp config.toml.example config.toml
vi config.toml        # endpoint, access_key, secret_key, bucket

# 3. Fire off a scan -> writes ./s3lister_out/part-*.parquet
./s3lister scan -config config.toml

# 4. Query it immediately with DuckDB (no import step)
duckdb -c "SELECT count(*), sum(size_bytes) FROM 's3lister_out/*.parquet'"
```

## Commands

### `scan`

Connects to S3, lists all objects under the configured bucket/prefix, and writes
the results as Parquet part files into the output directory.

```
./s3lister scan [options]

Options:
  -config string      Path to config file (default "config.toml")
  -readers int        Override number of reader threads
  -writers int        Override number of writer threads
  -bucket string      Override bucket from config
  -output string      Override output directory from config
  -tags               Also fetch every object's tags (see below)
  -tag-workers int    Override number of tag-fetch workers (with -tags)
  -verbose            Log to stderr and trace HTTP requests
```

Progress is shown as a live bar on the terminal; a status line is also written
to the log file every 5 seconds. Each writer produces one `part-NNN.parquet`
file, so `-writers 8` yields `part-000.parquet` … `part-007.parquet`.

Each run starts fresh: existing `part-*.parquet` files in the output directory
are removed before the scan begins.

#### Object tags (`-tags`) — opt-in

**Tag collection is OFF by default.** A plain `scan` makes zero tagging
calls and behaves exactly as it always has (every row gets `tags = NULL`,
`tag_count = -1`). To collect tags, add `-tags`:

```bash
./s3lister scan -config config.toml -bucket my-bucket -output ./out -tags
```

Why opt-in: S3 does not return tags in listings — they require a separate
`GetObjectTagging` call **per object**, a \~1000× request amplification over
listing. With `-tags`, a pool of tag-fetch workers (`tag_workers`, default
256; override with `-tag-workers`) sits between the listers and the Parquet
writers, and the scan runs at tag-fetch speed rather than listing speed.
Expect roughly `tag_workers ÷ per-request latency` objects/sec, and on AWS a
per-request cost (\~$0.40 per million objects). Scoping with `prefix` limits
the cost to the subtree you care about.

The output gains two columns: `tags`, a native Parquet map, and `tag_count`,
which disambiguates its states — `-1` tags not collected (scan without
`-tags`, or that object's fetch failed after retries; failures are counted,
logged per key, and make the scan exit non-zero), `0` object has no tags,
`N` object has N tags.

```sql
SELECT key, size_bytes FROM 'out/*.parquet'
WHERE key LIKE 'foofiles/%' AND list_contains(map_keys(tags), 'foo');
```

(That form works on every DuckDB version. Recent DuckDB also accepts the
terser `tags['foo'] IS NOT NULL` / `tags['env'] = 'prod'`, but on older
versions map indexing returns a list instead of the value — see
[docs/QUERY_DUCKDB.md](docs/QUERY_DUCKDB.md) for the full tag query set.)

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
| `tags` | MAP(VARCHAR, VARCHAR) | Object tags (only populated by `scan -tags`) |
| `tag_count` | INTEGER | `-1` = not collected, `0` = none, `N` = tag count |
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
readers     = 32             # goroutines listing S3 (ListObjectsV2)
writers     = 8              # goroutines writing Parquet (one file each)
queue_size  = 100000         # records buffered between readers and writers
tag_workers = 256            # goroutines fetching tags (only with scan -tags)

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

### Measured

Scans from a single Ubuntu client VM against a 6-VIP S3 endpoint, connections
spread across all VIPs by DNS discovery. Every result is DuckDB-verified
exact: `count(*) == count(DISTINCT key) ==` the number of objects populated.

| Objects | Wall time | Avg objs/s | Peak objs/s | Parquet on disk |
|---------|-----------|------------|-------------|-----------------|
| 5,000,000 | 10.6s | 470,442 | 483,125 | 43.7 MiB |
| 100,000,000 | 4m24s | 378,955 | 462,299 | 878.1 MiB |
| 2,000,000,000 | 1h20m33s | 413,799 | 733,128 | 17.0 GiB |

Throughput rises with scale — a larger keyspace gives the work-stealing
scheduler more parallelism to exploit — and the output stays \~9 bytes per
object. With `-tags`, the same client collected 100M objects *plus all
200M of their tags* in 36m35s (tag-fetch bound at one `GetObjectTagging`
per object; the tags cost \~2.3 extra bytes per object on disk).
Methodology, tuning, and full reproduction steps:
[bench-readme.md](bench-readme.md).

### Tuning notes

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
