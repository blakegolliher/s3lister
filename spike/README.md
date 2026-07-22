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
