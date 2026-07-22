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

## Benchmarking tag collection (`-tags`)

s3lister's tag collection (`scan -tags`) is opt-in and runs at
one-`GetObjectTagging`-call-per-object speed, so it gets its own benchmark
tier. We target **100M objects** for tag tests: large enough to be honest
about sustained tag-fetch throughput, small enough that a run completes in
well under an hour.

### Populate a tagged bucket

```bash
./s3lister-bench -bucket bench-100m-tags -count 100000000 -workers 512 -tags
```

`-tags` attaches tags at PUT time (they ride along in the same request, so
populate speed is unchanged). Like the key layout, the tag layout is a pure
function of the object index, which makes every distribution exact and
verifiable in advance:

- object `i` carries `i % 5` tags — 0 to 4, mean exactly 2
- tag keys rotate through an 8-key pool (`env`, `team`, `project`, `tier`,
  `owner`, `app`, `cost-center`, `retention`), each with 4 possible values
- for any `-count` divisible by 160: exactly `count/5` objects in each
  tag_count bucket, each key on exactly `count/4` objects, each key=value
  pair on exactly `count/16`

Resume and single-key repair work exactly as for untagged populates — just
keep passing `-tags` (the printed resume/repair commands include it).

### Scan with tags and verify

```bash
./s3lister scan -config config.toml -bucket bench-100m-tags -output ./out-100m-tags -tags
```

This is tag-fetch bound: expect roughly `tag_workers ÷ per-request latency`
objects/sec (tune with `-tag-workers`). Then verify — every number below is
exact for `-count 100000000`, not approximate:

```bash
duckdb -c "
  SELECT count(*) AS rows, count(DISTINCT key) AS uniq, sum(tag_count) AS total_tags
  FROM './out-100m-tags/*.parquet'"
# rows = uniq = 100000000, total_tags = 200000000

duckdb -c "
  SELECT tag_count, count(*) FROM './out-100m-tags/*.parquet'
  GROUP BY tag_count ORDER BY tag_count"
# exactly 20,000,000 objects in each bucket 0,1,2,3,4  (and none at -1:
# -1 anywhere means tag fetches failed — the scan will have said so loudly)

duckdb -c "
  SELECT t.key AS tag, count(*) FROM './out-100m-tags/*.parquet',
  unnest(map_entries(tags)) AS u(t) GROUP BY tag ORDER BY tag"
# each of the 8 keys on exactly 25,000,000 objects

duckdb -c "
  SELECT count(*) FROM './out-100m-tags/*.parquet'
  WHERE map_extract(tags, 'env') = ['prod']"
# exactly 6,250,000  (every key=value pair hits the same number)
```

A tags result only counts if all four checks land on the exact numbers.
Record the wall time and objects/sec alongside a note of `tag_workers`,
since that — not the listing engine — is what a `-tags` scan measures.

### Tags result

| Bucket | Objects | Wall time | Avg tags/s | Peak | Tag workers | Output | Exactness |
|--------|---------|-----------|------------|------|-------------|--------|-----------|
| bench-100m-tags | 100,000,000 | 36m35s | 45,560 | 58,062 | 256 | 1.1 GiB | ✓ all four checks exact |

Run from the same single 8-core client VM as the listing tiers, zero tag
errors and zero retries (`tag_retries=0` throughout — the storage system
never throttled). This number moved twice on the same VM and worker count:
the original SDK-based client held 18,051 tags/s with the CPU pinned at
load 8; `GOGC=400` recovered enough GC overhead for 23,776; and the direct
SigV4 client with the purpose-built XML parser reached 45,560 with CPU to
spare — at which point 256 workers are latency-bound (\~5.6ms per
`GetObjectTagging`), so further scaling is a worker-count and client-count
question, not a parsing one. Tags added \~220 MiB over the untagged 100M
output — about 2.3 bytes per object for 200M tags, thanks to the map
column's dictionary encoding.

## Repairing a populate that finished with errors

If a run ends with `N errors`, that many keys are missing from the bucket
and the exactness check will come up short by exactly N. Every key that
failed all retries is appended to the sidecar file (default
`./s3lister-bench.failed`), and keys embed their object index, so the repair
is to re-PUT precisely those indices — `-start i -count i+1` writes a single
key:

```bash
grep -o '[0-9]\{12\}' s3lister-bench.failed | while read i; do
  ./s3lister-bench -bucket bench-2b -count $((10#$i + 1)) -start $((10#$i)) -workers 1
done
```

No sidecar file (older binary)? The keyspace is deterministic, so DuckDB can
compute the missing indices from a scan of the bucket:

```sql
SET temp_directory='/path/with/room';   -- 2B-row anti-join spills to disk
COPY (
  SELECT i FROM range(0, 2000000000) t(i)
  WHERE NOT EXISTS (
    SELECT 1 FROM read_parquet('./out-2b/*.parquet') p
    WHERE CAST(regexp_extract(p.key, '(\d{12})', 1) AS BIGINT) = t.i
  )
) TO 'missing.txt' (HEADER false);
```

Feed `missing.txt` through the same loop, then re-scan for the verified
result.

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
- **Burst of `connection reset by peer` PUT errors mid-populate** — many
  simultaneous resets across several endpoint IPs is a server-side event:
  VIPs migrating between nodes (failover or rebalance) reset every
  established connection at once. Check the storage system's event log at
  the error timestamp. The populator retries each key with backoff for
  \~12s, which absorbs a normal failover; if keys still fail after retries
  it aborts at 1,000 failed keys and prints a `-start` resume point that is
  guaranteed to re-cover every failed key (chunks containing a failure
  never leave the resume window).
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
  -tags            Attach a deterministic variety of tags (0-4 per object) at
                   PUT time, for benchmarking `scan -tags`
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

## Results

Scans of buckets populated with the default layout (90% hierarchical /
10% flat), run from a single Ubuntu 24.04 client VM against a 6-VIP
S3 endpoint, connections spread across all VIPs via DNS discovery:

| Bucket | Objects | Wall time | Avg objs/s | Peak objs/s | Readers | Writers | Output size | Exactness |
|--------|---------|-----------|------------|-------------|---------|---------|-------------|-----------|
| bench-5m | 5,000,000 | 10.6s | 470,442 | 483,125 | 64 | 8 | 43.7 MiB | ✓ 5,000,000 |
| bench-100m | 100,000,000 | 4m24s | 378,955 | 462,299 | 64 | 8 | 878.1 MiB | ✓ 100,000,000 |
| bench-2b | 2,000,000,000 | 1h20m33s | 413,799 | 733,128 | 64 | 8 | 17.0 GiB | ✓ 2,000,000,000 |

Exactness is `count(*) == count(DISTINCT key) == objects populated`, checked
with DuckDB directly against the Parquet output. The check itself shows off
the format: the 100M-row distinct count completes in \~7 seconds on the
client VM, no import step, from 880 MiB of Parquet.

All tiers ran at
`-readers 64`, the knee on the test system: pushing to 128 or 256
readers *lowers* throughput (333k and 270k avg on the 100M tier) because
the storage system's per-page LIST latency grows with listing concurrency —
\~170ms per 1000-key page at 64 concurrent listers, \~950ms at 256 — while
the writers idle (`queued=0` throughout). Expect run-to-run variance from
the storage system's metadata cache state — at the 2B tier it dominates:
the record above was set on a hot cache (third consecutive pass), while
cold passes run 256–291k objs/s. The most recent cold pass, with the
direct-SigV4 client, verified exact in 1h54m32s with the client below
load 2 on 8 cores — at this scale the storage system's cache state, not
the client, decides the wall time.

## Reporting

For published numbers, record alongside the objects/sec figure:

- object count and layout flags (or "defaults")
- reader/writer counts used for the scan
- client hardware and network path to the endpoint
- the storage system under test and its configuration
- output size on disk (`ls -lh out-*/`) — the Parquet compresses to roughly
  9 bytes per object at these tiers (real-world key mixes with longer, more
  varied keys run 10–20)
- the DuckDB exactness check from step 5

## Cleaning up

Benchmark buckets contain millions to billions of keys; deleting them is
itself a large job and is intentionally out of scope for this tool. Use your
storage system's bucket-delete/expiry facilities rather than issuing per-key
deletes.
