# EXPLAIN (SQL)

`EXPLAIN (SQL)` returns executable DuckDB SQL reconstructed from a query's optimized logical plan:

```sql
EXPLAIN (SQL)
SELECT category, sum(amount) AS total
FROM sales
WHERE amount > 0
GROUP BY category;
```

The result has the usual `explain_key` and `explain_value` VARCHAR columns and one row. Its key is `sql`; its value is the generated query. Execute that value as a separate SQL statement to obtain the query result. The `explain_output` setting does not change this result.

The generated query preserves SQL semantics when rebound under the same catalog, settings, extensions, and external environment. It preserves output column names, column order, logical types, NULLs, duplicates, sampling, and ordering required by SQL. Without an ordering guarantee, row order may differ.

This is generated SQL, not recovery of the original query text. Optimizer rewrites can change expressions, join order, and query structure. Generated relation names and nested queries are expected. Formatting and SQL text are not stable across DuckDB versions. Rebinding may rediscover files, infer schemas, and observe current catalog or runtime state, as ordinary SQL does; the output is not a frozen execution snapshot or a portable catalog bundle.

Optimization can also fold observations made while planning into the generated SQL. A join input known to be empty may become a typed empty relation with `WHERE false`; `current_setting('threads')` may become a literal such as `CAST(10 AS BIGINT)`; `now()` may become a timestamp literal from the transaction in which the plan was optimized; and `count(*)` on a small table may become a literal derived from its statistics. Rebinding these literals does not repeat the observations that produced them. Export a plan optimized against the data and settings on which the generated query will run, including when exporting fragments for execution elsewhere; changes to those inputs may require optimizing and exporting a new plan.

## Supported statements and errors

SQL mode accepts supported SELECT, VALUES, and WITH queries. It does not combine with ANALYZE or FORMAT, and it does not support DDL/DML, CALL, or EXPLAIN EXECUTE. Existing restrictions on SQL PREPARE also apply. Some logical operators and expressions cannot yet be exported; their errors describe the unsupported query or source. The C++ export result retains structured issue codes, phases, and plan locations. Unsupported export never returns the original query as a fallback.

PIVOT with values discovered from data is rejected because column discovery requires executing auxiliary statements. Specify the values with an explicit IN list to export supported PIVOT queries. Unresolved parameters and explicit optimizer opt-outs may also leave unsupported plan nodes.

MARK joins, used by queries such as `IN` and `ANY`, support ordinary scalar comparisons and conjunctions containing only equality or only `IS NOT DISTINCT FROM` comparisons. `IS DISTINCT FROM`, ordering comparisons on nested types, mixed or arbitrary predicates, and plans requiring group-specific NULL handling or duplicate-eliminated input scopes are rejected. These restrictions preserve the distinction between false and unknown results.

## Execution and effects

Explaining does not execute the query or its generated replacement. Ordinary binding and optimization still occur, including catalog lookup and source schema discovery. Representable runtime or effecting calls may appear in the returned SQL:

```sql
CREATE SEQUENCE example_sequence;
EXPLAIN (SQL) SELECT nextval('example_sequence');
```

Explanation does not advance the sequence. Executing the returned query advances it once. Queries that cannot preserve observable evaluation behavior are rejected explicitly. Source callbacks and operator SQL reconstruction methods must return owned SQL representations without executing their source or performing effects.

The debug SQL-export verifier is a separate testing facility. `EXPLAIN (SQL)` works without enabling it and does not run differential executions to establish correctness.

## Fragment export

The C++ exporter consumes an already planned logical tree. It does not choose an optimization stage or undo constant folding. Distributed fragment export is experimental: callers must preserve the catalog, data, settings, and transaction assumptions under which the input was planned. Exported field bindings belong to that plan; they are not stable identifiers shared between independent exports. Pre-folding export options and stable fragment identities are not provided by this API. Scalar and aggregate functions can provide unbind callbacks to reconstruct their specialized invocations. The exporter attaches aggregate modifiers to the returned call.

## Implementation layout

Logical operators implement SQL reconstruction through `ToSQL`, including extension operators. The shared export context owns alias allocation, ancestor tracking, and named-relation scope. Its implementations are grouped by sources, VALUES/chunks, relational operators, joins, CTEs, LIMIT, and PIVOT. Shared binding and relation construction live in `sql_export_scope.cpp`. LIMIT reconstruction returns its modifier together with the scalar-input relations it needs. The LIMIT operator materializes those relations once and substitutes their names only while exporting its child.

The expression exporter owns binding context and lambda reference scopes. Constant values use the shared `ConstantExpression::FromValue` conversion, including nested type metadata and aggregate states. The exporter adds result typing and structured diagnostics; function calls and window expressions have separate implementations. Ordinary table functions reconstruct their qualified retained invocation by default. Source-specific callbacks return only a table reference; scan projection, ordinality, sampling and predicates are applied centrally. Generic source reconstruction lives in `table_function_sql_export.cpp`.

Internal helper declarations live under `src/include/duckdb/planner/sql_export/`. Public entry points retain their headers directly under `duckdb/planner/`. General logical-plan verification and repeatability analysis remain separate planner facilities. Statement replacement verification lives in `src/main/client_verify.cpp`.

C++ tests under `test/sql_export/` follow these feature boundaries; shared fixtures live in the corresponding test-helper files. SQL regression tests live under `test/sql/sql_export/`.

The CI Query Verification configuration uses `debug_verify_statement='explain_sql'`.
It executes reconstructed SQL when export succeeds and falls back for explicitly
unsupported shapes. Errors from reconstruction and generated execution propagate normally. Focused SQL
tests use `debug_verify_statement='explain_sql_strict'`, which also rejects unsupported
shapes. Both modes execute the generated statement once against existing expected results.

Projection expressions consumed by a scan are currently unsupported when their SQL
provenance is no longer retained. The optimizer records this condition explicitly,
including across plan serialization; native pushdown remains enabled.
