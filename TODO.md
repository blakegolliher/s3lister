# TODO

## Benchmarking

Establish real, reproducible performance numbers now that output is Parquet.

- [ ] Full-bucket scan against a large real bucket; record objects, wall-clock,
      avg/peak objects/sec, peak memory, and total Parquet size on disk.
- [ ] Sweep `readers` (e.g. 16/32/64/128) and `writers` (e.g. 4/8/16) to find the
      throughput knee for a representative bucket.
- [ ] Compare on-disk size vs. the old Pebble output and vs. plain CSV/gzip.
- [ ] Measure `export-csv` throughput (records/sec) over the Parquet parts.
- [ ] Sample DuckDB query latencies over the output (overview, by-extension,
      largest objects) to confirm the schema's pushdown pays off.
- [ ] Add a Performance section to the README with the results table
      (model after nfs-walker's benchmark tables).
