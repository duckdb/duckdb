# Profiling notes: issue #21836

## Symptom

Single-row `INSERT … ON CONFLICT DO UPDATE` against a table with several large
`DOUBLE[N]` columns could allocate gigabytes (e.g. ~3.7 GiB reported) under a
modest `memory_limit`.

## Plan shape

`INSERT … ON CONFLICT` is rewritten to `MERGE INTO` (`bind_insert.cpp`). The
generated source subquery includes `DISTINCT ON (unique key columns)` with
`SELECT *`, which is planned as a **hash aggregate** whose non-key columns use
the **`first()`** aggregate (`plan_distinct.cpp`).

## Hot spot

For types that are not handled by the primitive `first()` specializations,
`GetFirstFunction` fell through to **`FirstVectorFunction`**, which builds
**sort keys** (`CreateSortKey` / `DecodeSortKey`) for each row. For large
**fixed-length arrays**, that path is far more expensive than copying the raw
child payload.

## Fix direction

Add a dedicated **`first()` / `last()`** implementation for **`ARRAY`** types
whose element type has **constant size**, copying the flat child data directly
instead of sort-key encoding (`first_last_any.cpp`).
