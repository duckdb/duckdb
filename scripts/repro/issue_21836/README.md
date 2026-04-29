# Reproduction: issue #21836 (INSERT … ON CONFLICT, large fixed arrays)

This folder contains a minimal reproduction for
[duckdb/duckdb#21836](https://github.com/duckdb/duckdb/issues/21836): pathological
memory use when upserting rows with large `DOUBLE[N]` columns.

## Files

- `repro.sql` — minimal SQL repro (adjust `N` and `memory_limit` as needed).
- `explain.sql` — `EXPLAIN` for the rewritten plan (shows `HASH_GROUP_BY` / `first`
  over array columns from the `DISTINCT ON` layer).

## Usage

From the repository root, with a `duckdb` binary on `PATH` or
`build/release/duckdb` / `build/debug/duckdb`:

```bash
duckdb < scripts/repro/issue_21836/repro.sql
duckdb < scripts/repro/issue_21836/explain.sql
```

On a fixed tree, a successful upsert should return one row with `b[1] = 1.0` and
preserved `a[1] = 0.0`.
