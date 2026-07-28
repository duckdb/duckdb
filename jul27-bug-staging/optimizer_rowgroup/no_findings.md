# Optimizer / Row Group Pruning Bug Hunt - No Confirmed Findings

- Lane: optimizer, row group pruning, segment elimination, zone maps, filter pushdown, bad query plans
- Repository: `/Users/hjiang/Desktop/duckdb`
- Binary used: `build/reldebug/duckdb`
- DuckDB version observed: `v1.6.0-dev10443` / `b641ea77e6`
- Run window: approximately `2026-07-28 03:47 PDT` through `2026-07-28 03:49 PDT`
- Final timestamp: `2026-07-28 03:49:48 PDT` (`2026-07-28T10:49:48Z`)

No severe performance issue or incorrect-result bug was confirmed in this lane during this pass.

## Attempt 1: Native storage string collation and zone maps

Purpose: check whether binary string zone maps incorrectly prune case-insensitive predicates.

```sql
PRAGMA version;
CREATE TABLE t AS SELECT 'A'::VARCHAR s FROM range(10000);
SELECT count(*) AS plain_eq FROM t WHERE s = 'a';
SELECT count(*) AS nocase_eq FROM t WHERE s COLLATE NOCASE = 'a';
EXPLAIN SELECT count(*) FROM t WHERE s COLLATE NOCASE = 'a';
```

Observed:

- `s = 'a'` returned `0`.
- `s COLLATE NOCASE = 'a'` returned `10000`.
- Plan rewrote the predicate to `lower(s) = 'a'`.

Persistent row-group variant:

```sql
DROP TABLE IF EXISTS t;
CREATE TABLE t AS
SELECT CASE WHEN i < 200000 THEN 'A' ELSE 'z' END::VARCHAR s, i
FROM range(400000) tbl(i);
CHECKPOINT;
SELECT count(*) AS nocase_a FROM t WHERE s COLLATE NOCASE = 'a';
SELECT count(*) AS lower_a FROM t WHERE lower(s) = 'a';
SELECT count(*) AS plain_a FROM t WHERE s = 'a';
PRAGMA storage_info('t');
EXPLAIN ANALYZE SELECT count(*) FROM t WHERE s COLLATE NOCASE = 'a';
```

Observed:

- `nocase_a` and `lower_a` both returned `200000`.
- `plain_a` returned `0`.
- `EXPLAIN ANALYZE` reported `Row Groups Scanned: 4 / 4`, so no unsafe pruning was applied to the rewritten case-insensitive predicate.

Status: unconfirmed; behavior appears correct.

## Attempt 2: Native numeric/date row-group pruning

Purpose: confirm basic min/max pruning on monotonic columns and null-aware elimination.

```sql
DROP TABLE IF EXISTS nums;
CREATE TABLE nums AS
SELECT
    i::BIGINT AS id,
    (DATE '2000-01-01' + i::INTEGER) AS d,
    CASE WHEN i < 122880 THEN NULL ELSE i END::BIGINT AS nullable_id
FROM range(500000) tbl(i);
CHECKPOINT;

SELECT count(*) FROM nums WHERE id BETWEEN 400000 AND 400010;
EXPLAIN ANALYZE SELECT count(*) FROM nums WHERE id BETWEEN 400000 AND 400010;

SELECT count(*) FROM nums WHERE d = DATE '2000-01-10';
EXPLAIN ANALYZE SELECT count(*) FROM nums WHERE d = DATE '2000-01-10';

SELECT count(*) FROM nums WHERE nullable_id IS NULL;
EXPLAIN ANALYZE SELECT count(*) FROM nums WHERE nullable_id IS NULL;

SELECT count(*) FROM nums WHERE nullable_id = 1;
EXPLAIN ANALYZE SELECT count(*) FROM nums WHERE nullable_id = 1;
```

Observed:

- `id BETWEEN 400000 AND 400010` returned `11` and scanned `1 / 5` row groups.
- `d = DATE '2000-01-10'` returned `1` and scanned `1 / 5` row groups.
- `nullable_id IS NULL` returned `122880`.
- `nullable_id = 1` returned `0` and planned as `Empty Result`.

Status: unconfirmed; behavior appears correct.

## Attempt 3: Floating-point NaN/infinity segment metadata

Purpose: check whether unusual NaN/infinity zone-map stats cause incorrect pruning.

```sql
PRAGMA threads=1;
DROP TABLE IF EXISTS floats;
CREATE TABLE floats(x DOUBLE, payload INTEGER);
INSERT INTO floats
SELECT
    CASE
        WHEN i < 122880 THEN 'NaN'::DOUBLE
        WHEN i < 245760 THEN '-Infinity'::DOUBLE
        WHEN i < 368640 THEN i::DOUBLE
        ELSE 'Infinity'::DOUBLE
    END,
    i::INTEGER
FROM range(500000) tbl(i);
CHECKPOINT;

SELECT count(*) AS isnan_count FROM floats WHERE isnan(x);
SELECT count(*) AS eq_nan_count FROM floats WHERE x = 'NaN'::DOUBLE;
SELECT count(*) AS gt_big FROM floats WHERE x > 490000;
SELECT count(*) AS finite_range FROM floats WHERE x BETWEEN 300000 AND 300010;
EXPLAIN ANALYZE SELECT count(*) FROM floats WHERE x BETWEEN 300000 AND 300010;
EXPLAIN ANALYZE SELECT payload FROM floats WHERE x > 490000 LIMIT 5;
PRAGMA storage_info('floats');

PRAGMA disable_optimizer;
SELECT count(*) AS gt_big_noopt FROM floats WHERE x > 490000;
```

Observed:

- `isnan(x)` returned `122880`.
- `x = 'NaN'::DOUBLE` returned `122880`.
- `x > 490000` returned `254240`, matching `PRAGMA disable_optimizer`.
- `x BETWEEN 300000 AND 300010` returned `11` and scanned `1 / 5` row groups.
- `x > 490000 LIMIT 5` scanned `3 / 5` row groups and returned NaN rows first, consistent with DuckDB comparison semantics observed in the query output.
- `PRAGMA storage_info` showed unusual-looking NaN-only stats such as `min: inf`, `max: nan`, but no incorrect result was reproduced.

Status: unconfirmed; metadata display is suspicious-looking but observed query behavior was correct.

## Attempt 4: Parquet row-group pruning and filter pushdown

Purpose: check file-format pruning using Parquet row-group metadata.

Note: `INSTALL parquet` first failed in the sandbox because it attempted to create `/Users/hjiang/.duckdb/extensions/b641ea77e6`. Retried successfully with `LOAD parquet` using the available in-tree extension.

```sql
LOAD parquet;
DROP TABLE IF EXISTS psrc;
CREATE TABLE psrc AS
SELECT
    i::BIGINT AS id,
    CASE WHEN i < 100000 THEN 'A' WHEN i < 200000 THEN 'm' ELSE 'z' END::VARCHAR AS s,
    CASE WHEN i % 1000 = 0 THEN NULL ELSE i END::BIGINT AS n
FROM range(300000) tbl(i);

COPY psrc TO '/private/tmp/duckdb_prune_probe.parquet'
    (FORMAT parquet, ROW_GROUP_SIZE 50000);

SELECT count(*) AS id_slice
FROM read_parquet('/private/tmp/duckdb_prune_probe.parquet')
WHERE id BETWEEN 250000 AND 250010;

EXPLAIN ANALYZE
SELECT count(*)
FROM read_parquet('/private/tmp/duckdb_prune_probe.parquet')
WHERE id BETWEEN 250000 AND 250010;

SELECT count(*) AS impossible
FROM read_parquet('/private/tmp/duckdb_prune_probe.parquet')
WHERE id = -1;

EXPLAIN ANALYZE
SELECT count(*)
FROM read_parquet('/private/tmp/duckdb_prune_probe.parquet')
WHERE id = -1;

SELECT count(*) AS nocase_a
FROM read_parquet('/private/tmp/duckdb_prune_probe.parquet')
WHERE s COLLATE NOCASE = 'a';

EXPLAIN ANALYZE
SELECT count(*)
FROM read_parquet('/private/tmp/duckdb_prune_probe.parquet')
WHERE s COLLATE NOCASE = 'a';
```

Observed:

- `id BETWEEN 250000 AND 250010` returned `11` and scanned `1 / 6` row groups.
- `id = -1` returned `0` and planned as `Empty Result`.
- `s COLLATE NOCASE = 'a'` returned `100000` and scanned `6 / 6` row groups with predicate `lower(s) = 'a'`.

Status: unconfirmed; behavior appears correct.

## Attempt 5: Boundary integer types and simplification

Purpose: check signed/unsigned boundary rewrites and pruning.

```sql
DROP TABLE IF EXISTS bounds;
CREATE TABLE bounds AS
SELECT
    i::BIGINT AS id,
    ((i % 256) - 128)::TINYINT AS ti,
    (i % 256)::UTINYINT AS uti,
    (i - 250000)::INTEGER AS si
FROM range(500000) tbl(i);
CHECKPOINT;

SELECT count(*) AS ti_min FROM bounds WHERE ti = -128;
SELECT count(*) AS ti_gt_max FROM bounds WHERE ti > 127;
EXPLAIN ANALYZE SELECT count(*) FROM bounds WHERE ti > 127;

SELECT count(*) AS uti_neg FROM bounds WHERE uti = -1;
EXPLAIN ANALYZE SELECT count(*) FROM bounds WHERE uti = -1;

SELECT count(*) AS si_slice FROM bounds WHERE si BETWEEN -10 AND 10;
EXPLAIN ANALYZE SELECT count(*) FROM bounds WHERE si BETWEEN -10 AND 10;

PRAGMA disable_optimizer;
SELECT count(*) AS noopt_ti_gt_max FROM bounds WHERE ti > 127;
SELECT count(*) AS noopt_uti_neg FROM bounds WHERE uti = -1;
SELECT count(*) AS noopt_si_slice FROM bounds WHERE si BETWEEN -10 AND 10;
```

Observed:

- `ti = -128` returned `1954`.
- `ti > 127` returned `0`, matched non-optimized execution, and planned as `Empty Result`.
- `uti = -1` returned `0`, matched non-optimized execution, and planned as `Empty Result`.
- `si BETWEEN -10 AND 10` returned `21`, matched non-optimized execution, and scanned `1 / 5` row groups.

Status: unconfirmed; behavior appears correct.

## Attempt 6: Updates/deletes with stale-looking persisted zone maps

Purpose: verify that updated values outside a persisted row group's original min/max are not incorrectly pruned.

```sql
DROP TABLE IF EXISTS upd;
CREATE TABLE upd AS
SELECT i::BIGINT AS id, 'old'::VARCHAR AS tag
FROM range(500000) tbl(i);
CHECKPOINT;

UPDATE upd SET id=999999, tag='moved' WHERE id=42;

SELECT count(*) AS moved_count FROM upd WHERE id=999999;
EXPLAIN ANALYZE SELECT tag FROM upd WHERE id=999999;

DELETE FROM upd WHERE id BETWEEN 400000 AND 499999;

SELECT count(*) AS deleted_range FROM upd WHERE id BETWEEN 400000 AND 499999;
EXPLAIN ANALYZE SELECT count(*) FROM upd WHERE id BETWEEN 400000 AND 499999;
PRAGMA storage_info('upd');
```

Observed:

- `id=999999` returned `1` after updating `id=42` in the first row group.
- The lookup scanned `1 / 5` row groups and returned the updated row despite the persisted storage stats for row group 0 still showing the original min/max with `has_updates=true`.
- The deleted range returned `0`.
- The deleted range scan still touched `3 / 5` row groups, but this was not confirmed as a bug because the zone maps do not encode delete-vector emptiness for that predicate.

Status: unconfirmed; correctness appears intact.

## Conclusion

No confirmed bug report was written for this pass. The strongest lead was unusual NaN-only `DOUBLE` segment stats display, but all tested predicates matched expected and non-optimized results, so it is not filed as a confirmed release-blocking bug.
