# TODO

## Benchmarking

Establish real, reproducible performance numbers now that output is Parquet.

- [x] Full-bucket scan against a large real bucket; record objects, wall-clock,
      avg/peak objects/sec, and total Parquet size on disk.
      Done at 5M / 100M / 2B — results table in bench-readme.md. (Peak memory
      still unmeasured.)
- [ ] Sweep `readers` (e.g. 16/32/64/128) and `writers` (e.g. 4/8/16) to find the
      throughput knee for a representative bucket.
      - 2026-07-21: 2B at `-readers 64` → 413,799 avg / 733,128 peak
        (vs ~256k avg at 32, writers idle both times). Caveat: the 64-reader
        run was the third consecutive pass, so cluster metadata caches were
        warm — an earlier 64-reader pass over a colder cache averaged 264k.
        Next knee check: `-readers 128`.
- [ ] Compare on-disk size vs. the old Pebble output and vs. plain CSV/gzip.
- [ ] Measure `export-csv` throughput (records/sec) over the Parquet parts.
- [ ] Sample DuckDB query latencies over the output (overview, by-extension,
      largest objects) to confirm the schema's pushdown pays off.
- [x] Add a Performance section to the README with the results table
      (model after nfs-walker's benchmark tables).
