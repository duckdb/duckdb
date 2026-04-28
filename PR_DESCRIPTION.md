# Optimizer: fold struct_extract(struct_pack(...)) for table subscript

Table bracket access (`alias['col']`, including `getvariable('col')` after bind) was planned as `struct_extract` over a `struct_pack` of **all** columns, so scans could not prune columns (notably remote Parquet / many HTTP reads — [issue #21492](https://github.com/duckdb/duckdb/issues/21492)).

Add `StructExtractStructPackFoldingRule` in the expression rewriter to replace `struct_extract(struct_pack(e₀…eₙ), k)` with the bound child `eᵢ`.

**Tests:** `test/issues/general/test_21492.test`

**Also:** fix `first_last_any` aggregate bind (`ValidityMutable`, `GetArguments()`) so the tree builds.

Closes https://github.com/duckdb/duckdb/issues/21492
