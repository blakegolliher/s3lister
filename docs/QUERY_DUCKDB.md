# Querying Scans with DuckDB

`s3lister scan` writes a directory of `part-NNN.parquet` files. DuckDB reads the
whole set as a single table — **no import or conversion step**. Just point a glob
at the output directory.

```bash
duckdb
```

```sql
-- Treat the whole scan as one table
SELECT count(*) FROM 's3lister_out/*.parquet';
```

Prefer not to type the glob every time? Create a view:

```sql
CREATE VIEW objects AS SELECT * FROM 's3lister_out/*.parquet';
```

All examples below assume that `objects` view.

## Schema

Each row is one S3 object. Derived columns (`object_name`, `extension`,
`parent_prefix`, `depth`) are precomputed at scan time so filters get Parquet
predicate/statistics pushdown instead of per-row string parsing.

| Column | Type | Notes |
|--------|------|-------|
| `key` | VARCHAR | Full object key, e.g. `data/2024/report.csv` |
| `object_name` | VARCHAR | Basename after the last `/` |
| `extension` | VARCHAR | Text after the last `.` in the name (`""` if none) |
| `parent_prefix` | VARCHAR | Everything before the last `/` (`""` at top level) |
| `depth` | INTEGER | Number of `/` separators in the key |
| `size_bytes` | BIGINT | Object size in bytes |
| `last_modified` | TIMESTAMP | Object last-modified time (UTC) |
| `etag` | VARCHAR | Object ETag, quotes stripped |
| `storage_class` | VARCHAR | e.g. `STANDARD`, `GLACIER` (endpoint-dependent) |
| `scan_id` | VARCHAR | Identifier for the scan run, e.g. `scan-20260714T153000Z` |
| `scan_timestamp` | TIMESTAMP | When the scan started (UTC) |

## Overview Statistics

```sql
SELECT
    count(*)                                   AS objects,
    round(sum(size_bytes) / 1e9, 2)            AS total_gb,
    round(avg(size_bytes) / 1e6, 2)            AS avg_mb,
    max(depth)                                 AS max_depth,
    min(last_modified)                         AS oldest,
    max(last_modified)                         AS newest
FROM objects;
```

## Objects and Bytes by Extension

```sql
SELECT
    coalesce(nullif(extension, ''), '(none)')  AS ext,
    count(*)                                    AS objects,
    round(sum(size_bytes) / 1e9, 2)             AS size_gb
FROM objects
GROUP BY ext
ORDER BY size_gb DESC
LIMIT 20;
```

## Largest Objects

```sql
SELECT
    key,
    round(size_bytes / 1e9, 3)  AS size_gb,
    last_modified
FROM objects
ORDER BY size_bytes DESC
LIMIT 20;
```

## Fullest Prefixes ("directories")

```sql
SELECT
    parent_prefix,
    count(*)                        AS objects,
    round(sum(size_bytes) / 1e9, 2) AS size_gb
FROM objects
GROUP BY parent_prefix
ORDER BY objects DESC
LIMIT 20;
```

## Storage by Top-Level Prefix

```sql
SELECT
    split_part(key, '/', 1)          AS top_prefix,
    count(*)                          AS objects,
    round(sum(size_bytes) / 1e9, 2)   AS size_gb
FROM objects
GROUP BY top_prefix
ORDER BY size_gb DESC
LIMIT 20;
```

## Distribution by Storage Class

```sql
SELECT
    coalesce(nullif(storage_class, ''), '(unknown)') AS storage_class,
    count(*)                                          AS objects,
    round(sum(size_bytes) / 1e9, 2)                   AS size_gb
FROM objects
GROUP BY storage_class
ORDER BY size_gb DESC;
```

## Oldest Objects

```sql
SELECT key, last_modified, round(size_bytes / 1e6, 2) AS size_mb
FROM objects
ORDER BY last_modified ASC
LIMIT 20;
```

## Objects Modified in the Last 30 Days

```sql
SELECT count(*) AS recent_objects, round(sum(size_bytes) / 1e9, 2) AS size_gb
FROM objects
WHERE last_modified >= now() - INTERVAL 30 DAY;
```

## Size Histogram

```sql
SELECT
    CASE
        WHEN size_bytes = 0            THEN '0 (empty)'
        WHEN size_bytes < 1024        THEN '<1 KiB'
        WHEN size_bytes < 1<<20       THEN '1 KiB–1 MiB'
        WHEN size_bytes < 1<<30       THEN '1 MiB–1 GiB'
        ELSE '>1 GiB'
    END                               AS bucket,
    count(*)                          AS objects,
    round(sum(size_bytes) / 1e9, 2)   AS size_gb
FROM objects
GROUP BY bucket
ORDER BY min(size_bytes);
```

## Objects by Depth

```sql
SELECT depth, count(*) AS objects, round(sum(size_bytes) / 1e9, 2) AS size_gb
FROM objects
GROUP BY depth
ORDER BY depth;
```

## Empty (Zero-Byte) Objects

```sql
SELECT key, last_modified
FROM objects
WHERE size_bytes = 0
ORDER BY key
LIMIT 50;
```

## Potential Duplicates by ETag

For single-part uploads an S3 ETag is the MD5 of the content, so equal ETags
often mean identical objects. (Multipart-uploaded objects have a `-N` suffix and
are not comparable this way.)

```sql
SELECT
    etag,
    count(*)                          AS copies,
    round(sum(size_bytes) / 1e9, 2)   AS wasted_gb
FROM objects
WHERE etag <> '' AND etag NOT LIKE '%-%'
GROUP BY etag
HAVING copies > 1
ORDER BY wasted_gb DESC
LIMIT 20;
```

## Comparing Two Scans

Because every row carries `scan_id`, you can point the glob at several scans and
diff them. For example, keys present in the newer scan but not the older:

```sql
WITH s AS (SELECT * FROM 's3lister_out/*.parquet')
SELECT key
FROM s
WHERE scan_id = 'scan-20260714T153000Z'
EXCEPT
SELECT key
FROM s
WHERE scan_id = 'scan-20260701T090000Z'
LIMIT 50;
```

## Exporting Query Results

DuckDB can write results straight back out to CSV, Parquet, or JSON:

```sql
COPY (
    SELECT key, size_bytes, last_modified
    FROM objects
    WHERE size_bytes > 1<<30
) TO 'large_objects.csv' (HEADER, DELIMITER ',');
```

## One-Liners

Run a query without an interactive session:

```bash
duckdb -c "SELECT count(*), sum(size_bytes) FROM 's3lister_out/*.parquet'"
duckdb -c "SELECT extension, count(*) FROM 's3lister_out/*.parquet' GROUP BY extension ORDER BY 2 DESC LIMIT 10"
```

Other engines read the same files unchanged — `pyarrow`, `polars`, `pandas`,
Spark, Athena, ClickHouse, and `clickhouse-local` all take the Parquet glob
directly.
