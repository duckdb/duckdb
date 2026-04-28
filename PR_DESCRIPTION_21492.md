**PR title:** Optimizer: fold `struct_extract(struct_pack(...))` for table subscripts

## Description

Bracket access on a table alias (`alias['col']`, including `getvariable('col')` after bind) was planned as `struct_extract` over a full-row `struct_pack`, which blocked column pruning on scans (e.g. remote Parquet — [duckdb/duckdb#21492](https://github.com/duckdb/duckdb/issues/21492)).

- Add `StructExtractStructPackFoldingRule`: fold `struct_extract(struct_pack(e₀…eₙ), k)` → bound child `eᵢ`.
- Test: `test/issues/general/test_21492.test`.

Fixes https://github.com/duckdb/duckdb/issues/21492
