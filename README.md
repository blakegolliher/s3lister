# s3lister

A high-performance S3 object lister that records every object's full key path, size, and last-modified date into a local Pebble database. Designed for buckets with tens of millions to billions of objects.

## Example
```
[bg@chewbacca s3lister]$ sudo ./s3lister scan -config ./config.toml
  listed=385061524  written=385061524  queued=0  rate=30193/s  elapsed=3h32m33.337s   s

Done! 385061524 objects in 3h32m36.772s (30185/s)
Database: ./s3lister.db
Log: /var/log/s3lister.log
[bg@chewbacca s3lister]$
```
## Quick Start

```bash
# Build
go build -o s3lister .

# Edit config with your credentials
cp config.toml myconfig.toml
vi myconfig.toml

# Scan a bucket
./s3lister scan -config myconfig.toml

# Export to CSV
./s3lister export-csv -db ./s3lister.db -out objects.csv
```

## Commands

### `scan`

Connects to S3, lists all objects under the configured bucket/prefix, and writes results to a Pebble database.

```
./s3lister scan [options]

Options:
  -config string   Path to config file (default "config.toml")
  -readers int     Override number of reader goroutines
  -writers int     Override number of writer goroutines
```

Progress is printed to stderr every 5 seconds:

```
  listed=1482039  written=1480000  queued=2039  rate=74102/s  elapsed=20s
```

### `export-csv`

Reads the Pebble database and writes a CSV file with columns: `key`, `size_bytes`, `last_modified`.

```
./s3lister export-csv [options]

Options:
  -db string    Path to Pebble database (default "./s3lister.db")
  -out string   Output CSV file path (default "s3objects.csv")
```

Progress is printed every 30 seconds. Output uses RFC 4180 CSV encoding via Go's standard library.

Example output:

```csv
key,size_bytes,last_modified
data/2024/01/report.parquet,10485760,2024-01-15T08:30:00Z
logs/app/2024-03-01.log.gz,524288,2024-03-01T00:00:12Z
```

## Configuration

All configuration lives in a TOML file. See `config.toml` for the template.

```toml
[s3]
access_key = "AKIAIOSFODNN7EXAMPLE"
secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
endpoint   = "https://s3.amazonaws.com"
bucket     = "my-bucket"
prefix     = ""              # optional: only list objects under this prefix
region     = "us-east-1"

[workers]
readers    = 32              # goroutines listing S3 (1-512)
writers    = 8               # goroutines writing to Pebble (1-128)
queue_size = 100000          # channel buffer between readers and writers

[storage]
db_path = "./s3lister.db"    # Pebble database directory

[logging]
log_file = "/var/log/s3lister.log"
```

### Tuning

| Scenario | Readers | Writers | Queue |
|----------|---------|---------|-------|
| Small bucket (<1M objects) | 8-16 | 4 | 50000 |
| Medium bucket (1M-100M) | 32 | 8 | 100000 |
| Large bucket (100M+) | 64-128 | 16 | 500000 |
| High-latency endpoint | 64-128 | 8 | 200000 |

Readers are network-bound (S3 API calls). Writers are I/O-bound (Pebble flushes). The queue decouples them so neither side blocks the other. When in doubt, add more readers.

### S3-Compatible Endpoints

Works with any S3-compatible API. Set `endpoint` accordingly:

- AWS S3: `https://s3.amazonaws.com`
- MinIO: `http://minio.local:9000`
- Ceph RGW: `http://rgw.local:8080`
- Wasabi: `https://s3.wasabisys.com`

## Logging

All operations are logged to the configured log file with microsecond timestamps. The log includes:

- Startup configuration
- Prefix discovery and splitting decisions
- Per-worker completion stats
- Slow operations (batches >100ms, listings >2s)
- Write errors
- Final totals and throughput

## Graceful Shutdown

Send `SIGINT` (Ctrl+C) or `SIGTERM` to stop. In-flight S3 pages will finish, the record channel will drain, and writers will flush remaining batches before exiting.

## Requirements

- Go 1.21+
- Network access to the S3 endpoint
- Write access to the log file path
