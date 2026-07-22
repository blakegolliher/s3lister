# Go vs Rust spike

Measures what a Rust port of s3lister's hot path would buy. Not shipped, not
supported — a decision-making tool. Both sides do identical work against the
deterministic bench-bucket layouts so the comparison is honest:

- **Go side**: `spike/go-fastlist` drives s3lister's real `FastClient`
  (direct SigV4, purpose-built XML parser) flat-out — no writers, no
  work-stealing, just the wire loop.
- **Rust side**: `spike/rust-fastlist` is the same parser ported line-for-line
  (zero-copy where Rust allows it), hand-rolled SigV4, tokio + reqwest for
  async IO, connections round-robined across every resolved endpoint IP.

http:// endpoints only (no TLS stack compiled into the Rust spike).

## Build

```bash
# Go side (from the repo root)
go build -o go-fastlist ./spike/go-fastlist

# Rust side (rustup.rs if cargo is missing)
cd spike/rust-fastlist && cargo build --release
```

## 1. Parse benchmark (CPU only, runs anywhere)

```bash
go test ./internal/s3client/ -bench BenchmarkParseListPage -benchmem -run xxx
spike/rust-fastlist/target/release/rust-fastlist bench-parse
```

Measured on an Apple M3 (same machine, same synthetic 1000-key page):

| Parser | ns/page | relative |
|--------|---------|----------|
| Go `encoding/xml` (what the AWS SDK does) | 5,118,842 | 0.10x |
| Go fastxml | 546,889 | 1.00x |
| Rust port, owned strings (same work as Go) | 305,926 | 1.79x |
| Rust port, zero-copy borrowed | 244,814 | 2.23x |

## 2. Wire benchmark (run on the client VM against the lab endpoint)

Both harnesses take the same shape of flags and print the same output format
(a rate line every 5s, then a TOTAL line). Run each for the same duration at
the same worker count, ideally back to back:

```bash
# Go
./go-fastlist -config config.toml -bucket bench-100m-tags -mode tags -workers 256 -seconds 120
./go-fastlist -config config.toml -bucket bench-100m-tags -mode list -workers 64  -seconds 120

# Rust (flags are explicit; paste endpoint/keys from config.toml)
spike/rust-fastlist/target/release/rust-fastlist bench-tags \
  --endpoint http://HOST --bucket bench-100m-tags \
  --access-key K --secret-key S --workers 256 --seconds 120
spike/rust-fastlist/target/release/rust-fastlist bench-list \
  --endpoint http://HOST --bucket bench-100m-tags \
  --access-key K --secret-key S --workers 64 --seconds 120
```

Record alongside each result: the TOTAL rate, `errors=` (must be 0 for the
number to count), and the load average during the run (`uptime`) — the whole
point is comparing throughput *at* the CPU ceiling, so a side that isn't
saturating the client is measuring the cluster, not the language.

`-mode list` paginates the 4,096 `data/dNNN/sNNN/` prefixes of the default
bench layout; `-mode tags` fetches tags for the deterministic bench keys
(`--count` must match the bucket's populated object count). Both wrap around,
so any `--seconds` works.

## Wire results (2026-07-22)

Lab client VM (8 cores) against the 6-VIP endpoint, 120s runs, errors=0
throughout:

| Workload | Go (FastClient) | Rust (tokio/reqwest) | Ratio |
|----------|-----------------|----------------------|-------|
| list, 64 workers | 484,180 objs/s | 495,813 objs/s | 1.02x |
| tags, 256 workers — cold metadata | 21,493/s | — | — |
| tags, 256 workers — warm metadata | 37,078/s | 39,237/s | 1.06x |

Two findings:

1. **The languages tie at every measured operating point.** Listing waits
   \~130ms per 1000-key page and tag reads \~6–12ms per object — both sides
   idle on the server, and the Rust list run's 0.5 load average just shows
   how little client CPU the wire loop needs (Go's is similarly low without
   the Parquet pipeline attached). An initial 1.83x "Rust win" on tags
   evaporated on an A/B/A rerun: it was VAST metadata-cache warmth — the
   first (Go) pass ran cold, the second (Rust) pass rode the path the first
   had just warmed.
2. **Access pattern beats language.** The real Go scanner fetches tags in
   listing order (strong per-directory locality) and sustains 45,560/s —
   faster than either harness's 37–39k in deliberately locality-hostile
   index order.

**Decision: no port.** With encoding/xml and the SDK middleware already
gone, the client is IO-bound; Rust's real parse edge (1.8–2.2x, table
above) has nothing to bite on. Revisit if a future client is CPU-saturated
at the wire — e.g. a many-core box pushing multiple million objs/s — or
run the 2048-worker tags pair to probe saturation if the question ever
reopens.

## Interpreting

The scan pipeline spends its CPU in three places: response parsing, request
machinery (build/sign/HTTP), and the Parquet writer. The parse benchmark
isolates the first; the wire benchmark measures the first two under real IO.
The writer is out of scope — arrow-rs vs parquet-go is a separate question,
and writers have never been the bottleneck at current rates.

A Rust rewrite makes sense only if the wire ratio at CPU saturation is well
above what allocation discipline can recover in Go — weigh it against
re-proving the exactly-once machinery (the verification harness transfers,
which is most of the de-risking).
