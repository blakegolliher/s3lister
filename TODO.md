# TODO

## Benchmarking

Establish real, reproducible performance numbers now that output is Parquet.

- [x] Full-bucket scan against a large real bucket; record objects, wall-clock,
      avg/peak objects/sec, and total Parquet size on disk.
      Done at 5M / 100M / 2B — results table in bench-readme.md. (Peak memory
      still unmeasured.)
- [x] Sweep `readers` (e.g. 16/32/64/128) to find the throughput knee.
      Knee found at 64 (2026-07-21, warm-cache passes):
      - 2B:   32 → \~256k avg;  64 → 413,799 avg (1h20m33s);
              128 → 408,932 avg (1h21m31s). 64 and 128 are a wash; the
              client VM pegs its CPU at 128 (progress-goroutine starvation
              was visible), so past 64 the single-VM client is the limit.
      - 100M: 32 → 304,762 avg; 128 → 273,393 avg (slower). 64 untested —
        might edge out 32 if anyone cares to close that gap.
      - 5M:   32 → 282,006 avg; 128 → 279,308 avg. Scan finishes in \~18s,
        too short for extra readers to matter.
- [ ] Sweep `writers` (e.g. 4/8/16) — never varied; all runs used 8 with
      writers mostly idle, so this only matters on beefier clients.
- [ ] Find the cluster's true tag-read ceiling: the 100M `-tags` run
      (18k tags/s, tag_retries=0) was client-CPU-bound on the 8-core VM.
      Needs a bigger client, or two clients scanning disjoint prefixes.
      Note: 1024 tag workers on the 8-core VM collapsed throughput
      (\~1k/s) — likely scheduler/GC thrash, worth confirming with the
      tag_retries counter before blaming the storage side.
- [ ] Retake the 2B record with the fast client on a warm cache (two
      consecutive passes; the standing 1h20m33s record was a warm third
      pass on the SDK client). Fast-client cold pass: 1h54m32s, verified
      exact, client below load 2.
- [ ] Compare on-disk size vs. the old Pebble output and vs. plain CSV/gzip.
- [ ] Measure `export-csv` throughput (records/sec) over the Parquet parts.
- [ ] Sample DuckDB query latencies over the output (overview, by-extension,
      largest objects) to confirm the schema's pushdown pays off.
- [x] Add a Performance section to the README with the results table
      (model after nfs-walker's benchmark tables).
