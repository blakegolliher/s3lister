# Benchmarking s3lister

This document is the end-to-end guide for benchmarking s3lister and
reproducing our published numbers. The tooling is a standalone binary,
`s3lister-bench`, kept separate from the main tool: s3lister exists to turn a
bucket into a queryable Parquet dataset as fast as possible, and its users
don't need bucket-population machinery along for the ride.

## End-to-end walkthrough

### 1. Clone and build both binaries

Requires Go 1.24+. Run the benchmark from a machine as close to the S3
endpoint as possible — client-side network latency is part of what you're
measuring.

```bash
git clone https://github.com/blakegolliher/s3lister.git
cd s3lister
go build -o s3lister .                            # the scanner being measured
go build -o s3lister-bench ./cmd/s3lister-bench   # the bucket populator
```

If `./cmd/s3lister-bench` doesn't exist, your checkout predates the bench
tooling — `git pull` first.

### 2. Configure endpoint and credentials

```bash
cp config.toml.example config.toml
vi config.toml    # set access_key, secret_key, endpoint
```

Only the `[s3]` endpoint/credentials/region matter for the benchmark flow.
Leave `bucket` as any placeholder — the commands below name their buckets
explicitly, and `s3lister-bench` creates them if they don't exist.

**Use the DNS name of the endpoint, not a single VIP.** Both binaries
resolve the hostname and rotate new connections across every IP it returns
(re-resolving every 30s), so a scale-out front end with many VIPs behind one
name gets even load. On startup the log records what was discovered:

```
[s3] endpoint main.selab... resolves to 16 address(es): 172.200.204.1, ...
```

Check that count matches the number of VIPs you expect the cluster to
publish — if it says 1, your DNS name only returns one address and every
connection lands on the same node.

### 3. Populate a benchmark bucket

```bash
./s3lister-bench -bucket bench-5m -count 5000000 -workers 512
```

For the larger tiers use `-count 100000000` (bench-100m) and
`-count 2000000000` (bench-2b). Populate throughput is bounded by the
target's PUT rate, not this tool — rough wall-clock at 30k PUT/s: 5M ≈ 3 min,
100M ≈ 1 h, 2B ≈ 18.5 h. Progress, rate, and ETA display live.

If the run is interrupted it prints the exact `-start` index to resume from.
For the 2B tier, consider splitting the keyspace across hosts — same
`-count`, disjoint `-start`:

```bash
hostA$ ./s3lister-bench -bucket bench-2b -count 1000000000 -workers 1024
hostB$ ./s3lister-bench -bucket bench-2b -count 2000000000 -start 1000000000 -workers 1024
```

### 4. Scan — this is the measured step

```bash
./s3lister scan -config config.toml -bucket bench-5m -output ./out-5m -readers 128
```

The scan prints wall time, average and peak objects/sec when it completes;
the same stats plus per-worker detail land in `s3lister.log`.

### 5. Verify exactness before believing any number

```bash
duckdb -c "SELECT count(*) AS rows, count(DISTINCT key) AS uniq FROM './out-5m/*.parquet'"
```

A result only counts if `rows == uniq ==` the `-count` you populated: every
object listed exactly once, no duplicates, no gaps. (No duckdb on the box?
`./s3lister export-csv -in ./out-5m -out keys.csv` and count lines.)

### 6. Repeat for each tier

Same three commands per tier — populate, scan, verify — for `bench-100m` and
`bench-2b`.

## Troubleshooting

- **`tls: failed to verify certificate: x509: certificate is valid for
  *.example.com, not <host>`** — the endpoint's certificate doesn't cover the
  hostname you're using (common in lab environments where a wildcard cert
  doesn't match nested subdomains). Use the hostname the certificate was
  issued for, or an `http://` endpoint if you're on a trusted lab network.
- **`S3 connectivity check failed ... HeadBucket ... NotFound`** — the bucket
  named in `config.toml` (or `-bucket`) doesn't exist on that endpoint. For
  the benchmark flow this is expected until step 3 creates it: `scan`
  verifies its bucket up front, but `s3lister-bench` creates its bucket, so
  populate first.
- **Endpoint unreachable / timeouts** — verify basic reachability with
  `curl -m 5 http://<endpoint>/` before debugging anything else.
- **All load lands on one node** — check what DNS actually returns:
  `dig +short <endpoint>` a few times in a row.
  - *Same single IP every query*: the name is a static record pointing at one
    VIP. No client can spread that — publish all VIPs under the name, or use
    the storage system's delegated/load-balancing DNS name.
  - *Different single IP each query*: server-side round-robin. The client
    re-resolves on every new connection in this mode (it logs
    `single-address DNS answer: re-resolving on every connection`), so
    connections follow the server's rotation.
  - *Many IPs per query*: the client rotates across all of them itself; the
    startup log lists what it found.

## s3lister-bench reference

```
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
  -flat-pct int    Percent of keys placed in large flat prefixes (0-100, default 10)
  -flat-dirs int   Number of flat prefixes (default 16)
  -log string      Log file (default "./s3lister-bench.log")
```

Objects are zero-byte by default because `ListObjectsV2` performance — the
thing being measured — does not depend on object size, and empty PUTs
populate the keyspace fastest.

## Key layout

Keys are a **pure function of the object index**, so a given set of flags
always produces exactly the same keyspace. That gives three properties that
matter for honest benchmarking:

1. **Reproducible** — anyone running the same flags gets a bit-identical
   bucket layout.
2. **Resumable** — an interrupted run resumes from the printed `-start`;
   re-PUTting an overlapping range is an idempotent overwrite.
3. **Verifiable** — the expected object count is known in advance, so the
   scan's output can be checked for exactness.

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

## Reporting

For published numbers, record alongside the objects/sec figure:

- object count and layout flags (or "defaults")
- reader/writer counts used for the scan
- client hardware and network path to the endpoint
- the storage system under test and its configuration
- output size on disk (`ls -lh out-*/`) — the Parquet compresses to roughly
  10–20 bytes per object
- the DuckDB exactness check from step 5

## Cleaning up

Benchmark buckets contain millions to billions of keys; deleting them is
itself a large job and is intentionally out of scope for this tool. Use your
storage system's bucket-delete/expiry facilities rather than issuing per-key
deletes.
