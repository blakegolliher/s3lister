# Benchmarking s3lister

This document describes how we benchmark s3lister and how to reproduce the
results. The tooling is a standalone binary, `s3lister-bench`, kept separate
from the main tool: s3lister exists to turn a bucket into a queryable Parquet
dataset as fast as possible, and its users don't need bucket-population
machinery along for the ride.

## Build

```bash
go build -o s3lister .                       # the scanner being measured
go build -o s3lister-bench ./cmd/s3lister-bench   # the bucket populator
```

## What s3lister-bench does

It fills a bucket with synthetic zero-byte objects at high concurrency,
creating the bucket if it doesn't exist. Object bodies are empty because
`ListObjectsV2` performance — the thing being measured — does not depend on
object size, and empty PUTs populate the keyspace fastest.

```
./s3lister-bench -bucket bench-100m -count 100000000 -workers 512

Options:
  -config string   Path to s3lister config file (default "config.toml";
                   only the endpoint/credentials/region are used)
  -bucket string   Target bucket (required; created if missing)
  -count int       Total number of objects the bucket should contain (required)
  -start int       Starting object index — resume an interrupted run, or split
                   one keyspace across multiple hosts
  -workers int     Concurrent PUT workers (default 256)
  -size int        Object body size in bytes (default 0 = empty, fastest)
  -dirs1 int       First-level directory fanout (default 64)
  -dirs2 int       Second-level directory fanout (default 64)
  -flat-pct int    Percent of keys placed in large flat prefixes (default 10)
  -flat-dirs int   Number of flat prefixes (default 16)
  -log string      Log file (default "./s3lister-bench.log")
```

## Key layout

Keys are a **pure function of the object index**, so a given set of flags
always produces exactly the same keyspace. That gives three properties that
matter for honest benchmarking:

1. **Reproducible** — anyone running the same flags gets a bit-identical
   bucket layout.
2. **Resumable** — an interrupted run prints the exact `-start` to resume
   from; re-PUTting an overlapping range is an idempotent overwrite.
3. **Verifiable** — the expected object count is known in advance, so the
   scan's output can be checked for exactness (see below).

The default layout mixes the two shapes that stress s3lister's two
parallelization strategies:

- **90% hierarchical**: `data/dNNN/sNNN/obj-NNNNNNNNNNNN.<ext>` spread across
  `dirs1 × dirs2` (default 4,096) directories with rotating realistic
  extensions. This exercises prefix discovery and work-stealing.
- **10% flat**: `flat/fNN/o-NNNNNNNNNNNN` — a handful of very large flat
  prefixes (millions of keys each at scale, no delimiters). This exercises
  dynamic range-splitting, the path that keeps one giant "directory" from
  serializing the whole scan.

This is deliberately *not* a best-case layout for s3lister. A purely
hierarchical bucket (`-flat-pct 0`) scans faster; the default keeps the
awkward flat-prefix case in the measurement.

## Benchmark procedure

Point `config.toml` at the endpoint under test, then for each scale:

```bash
# 1. Populate (5M shown; use 100000000 / 2000000000 for the larger runs)
./s3lister-bench -bucket bench-5m -count 5000000 -workers 512

# 2. Scan — this is the measured step
./s3lister scan -config config.toml -bucket bench-5m -output ./out-5m -readers 128

# 3. Verify exactness before believing any number
duckdb -c "SELECT count(*) AS rows, count(DISTINCT key) AS uniq FROM './out-5m/*.parquet'"
```

The scan prints wall time, average and peak objects/sec; the same stats plus
per-worker detail land in `s3lister.log`. A result only counts if step 3
reports `rows == uniq == count`: every object listed exactly once, no
duplicates, no gaps.

Populate throughput is bounded by the target's PUT rate, not by this tool —
rough wall-clock at 30k PUT/s: 5M ≈ 3 min, 100M ≈ 1 h, 2B ≈ 18.5 h. For the
2B bucket, consider splitting the populate across hosts: give every host the
same `-count` and a disjoint `-start` slice, e.g.

```bash
hostA$ ./s3lister-bench -bucket bench-2b -count 1000000000 -workers 1024
hostB$ ./s3lister-bench -bucket bench-2b -count 2000000000 -start 1000000000 -workers 1024
```

## Reporting

For published numbers, record alongside the objects/sec figure:

- object count and layout flags (or "defaults")
- reader/writer counts used for the scan
- client hardware and network path to the endpoint
- the storage system under test and its configuration
- output size on disk (`ls -lh out-*/`) — the Parquet compresses to roughly
  10–20 bytes per object
- the DuckDB exactness check from step 3

## Cleaning up

Benchmark buckets contain millions to billions of keys; deleting them is
itself a large job and is intentionally out of scope for this tool. Use your
storage system's bucket-delete/expiry facilities rather than issuing per-key
deletes.
