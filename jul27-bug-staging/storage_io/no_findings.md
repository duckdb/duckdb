# Storage/IO Bug Hunt - No Confirmed Findings

## Window

- Start: 2026-07-28 02:04:23 PDT
- End: 2026-07-28 03:49:54 PDT
- Build: `v1.6.0-dev10443 (Development Version) b641ea77e6`
- Lane: storage, compression/encoding, duplicate IO operations, parquet/csv/json scan behavior, checkpoint/WAL, row group metadata, compression regression.

## Result

No confirmed query crash, INTERNAL exception, incorrect result, leak, or severe obvious performance issue was found in this pass.

## Attempts

### Focused sqllogictests

All selected tests passed unless noted.

```bash
build/reldebug/test/unittest test/sql/copy/parquet/read_ahead/readahead_limit_slot_leak_hang.test
build/reldebug/test/unittest test/sql/copy/parquet/parquet_prefetch_row_group_outcome.test
build/reldebug/test/unittest test/sql/copy/csv/read_ahead/csv_readahead_multi_file.test
build/reldebug/test/unittest test/sql/storage/wal/wal_null_update_row_group_pruning.test
build/reldebug/test/unittest test/sql/storage/compression/alp/alp_corrupted_vector_size.test
build/reldebug/test/unittest test/sql/storage/compression/patas/patas_corrupted_file.test
build/reldebug/test/unittest test/sql/storage/compression/rle/rle_corrupted_count_offset.test
build/reldebug/test/unittest test/sql/copy/parquet/broken_parquet.test
build/reldebug/test/unittest test/sql/copy/csv/zstd_crash.test
```

`test/sql/copy/parquet/broken_parquet.test` passed with one expected skipped unstable mode.

### Persistent Compression/Checkpoint Mix

Created and reopened `jul27-bug-staging/storage_io/compression_mix.duckdb` with forced `bitpacking` and `fsst`, updates, deletes, and checkpoints.

Key checks:

```sql
PRAGMA threads=8;
PRAGMA force_compression='bitpacking';
CREATE OR REPLACE TABLE ints AS
SELECT i::INTEGER AS id,
       CASE WHEN i%17=0 THEN NULL ELSE (i%5)::INTEGER END AS grp,
       (i*13%100000)::BIGINT AS payload
FROM range(0, 500000) tbl(i);
CHECKPOINT;

PRAGMA force_compression='fsst';
CREATE OR REPLACE TABLE strs AS
SELECT i::INTEGER AS id,
       CASE WHEN i%11=0 THEN NULL ELSE repeat(chr(65+(i%26)::INTEGER), 1+(i%37)::INTEGER) END AS s
FROM range(0, 250000) tbl(i);
CHECKPOINT;

UPDATE ints SET grp=NULL WHERE id%97=0;
DELETE FROM ints WHERE id%101=0;
CHECKPOINT;

SELECT count(*), sum(id), sum(payload), count(*) FILTER (WHERE grp IS NULL) FROM ints;
SELECT count(*), min(length(s)), max(length(s)), count(*) FILTER (WHERE s IS NULL) FROM strs;
SELECT column_name, compression, count(*) FROM pragma_storage_info('ints') GROUP BY ALL ORDER BY 1,2;
```

Observed stable aggregates and plausible storage metadata after checkpoint/reopen. No crash or incorrect result confirmed.

### Parquet Scan and Row Group Metadata

Created `jul27-bug-staging/storage_io/p_mix.parquet` with ZSTD compression and 4096-row row groups, then scanned nested struct data and metadata.

```sql
CREATE OR REPLACE TABLE p AS
SELECT i::INTEGER AS id,
       i%13 AS k,
       CASE WHEN i%19=0 THEN NULL ELSE 'v'||(i%100)::VARCHAR END AS v,
       {'a': i, 'b': [i, i+1, NULL]} AS st
FROM range(0, 200000) tbl(i);

COPY p TO 'jul27-bug-staging/storage_io/p_mix.parquet'
  (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 4096);

SELECT count(*), sum(id), count(v), sum(st.a)
FROM read_parquet('jul27-bug-staging/storage_io/p_mix.parquet');

SELECT count(*), sum(id)
FROM read_parquet('jul27-bug-staging/storage_io/p_mix.parquet')
WHERE k IN (3,7,11) AND id BETWEEN 1000 AND 180000;

SELECT file_name, row_group_id, row_group_num_rows
FROM parquet_metadata('jul27-bug-staging/storage_io/p_mix.parquet')
WHERE path_in_schema='id'
ORDER BY row_group_id
LIMIT 5;
```

Observed expected row counts, sums, and row group metadata. No bug confirmed.

### CSV/JSON Scan Round Trip

Generated CSV gzip and newline-delimited JSON from the same table and compared scan aggregates.

```sql
CREATE OR REPLACE TABLE c AS
SELECT i::INTEGER AS id,
       CASE WHEN i%23=0 THEN NULL ELSE (i%1000)::VARCHAR END AS txt,
       CASE WHEN i%29=0 THEN NULL ELSE i::DOUBLE/7 END AS val
FROM range(0, 120000) tbl(i);

COPY c TO 'jul27-bug-staging/storage_io/c_mix.csv.gz' (HEADER, COMPRESSION gzip);
COPY c TO 'jul27-bug-staging/storage_io/j_mix.ndjson' (FORMAT json);

SELECT count(*), sum(id), count(txt), round(sum(val),3)
FROM read_csv('jul27-bug-staging/storage_io/c_mix.csv.gz', header=true);

SELECT count(*), sum(id), count(txt), round(sum(val),3)
FROM read_json('jul27-bug-staging/storage_io/j_mix.ndjson', format='newline_delimited');
```

CSV and JSON aggregates matched. No incorrect result confirmed.

### Parquet Union/Virtual Column Differential

Compared native table expectations with `read_parquet(..., union_by_name=true, filename=true, file_row_number=true)` across overlapping files with different schemas.

Observed matching total count (`151072`) against the SQL baseline and stable filtered aggregates. No bug confirmed.

### WAL/Checkpoint Reopen

Created `jul27-bug-staging/storage_io/wal_checkpoint_mix.duckdb`, performed checkpoint, update, delete, insert, then reopened the database and compared aggregates.

Observed aggregate before reopen:

```text
count=106086, sum(id)=6073816957, count(s)=94435, round(sum(v),3)=1429207303.0
```

Observed the same aggregate after reopen. No WAL/checkpoint durability issue confirmed.

### Low-Memory Scan Attempt

An intentionally constrained run with `PRAGMA memory_limit='64MB'` and a wide generated string table failed during data creation with a normal `Out of Memory Error`. This was not counted as a bug.

A smaller follow-up at `128MB`, `threads=2`, and `preserve_insertion_order=false` completed successfully, with matching CSV ZSTD and Parquet ZSTD scan aggregates:

```text
count=220000, sum(id)=24199890000, sum(length(s))=34099000
```

### Invalid Verification Pragma

Tried `PRAGMA verify_external` during follow-up validation. This build does not expose that pragma and returned a catalog error. This was not counted as a bug.
