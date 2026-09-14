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

Optimization can also fold observations made while planning into the generated SQL. A join input known to be empty may become a typed empty relation with `WHERE false`; `current_setting('threads')` may become a literal such as `CAST(10 AS BIGINT)`; and `count(*)` on a small table may become a literal derived from its statistics. Rebinding these literals does not repeat the observations that produced them. Export a plan optimized against the data and settings on which the generated query will run, including when exporting fragments for execution elsewhere; changes to those inputs may require optimizing and exporting a new plan.

## Supported statements and errors

SQL mode accepts supported SELECT, VALUES, and WITH queries. It does not combine with ANALYZE or FORMAT, and it does not support DDL/DML, CALL, or EXPLAIN EXECUTE. Existing restrictions on SQL PREPARE also apply. Some logical operators and expressions cannot yet be exported; their errors identify the export issue, phase, location in the plan, and reason. Unsupported export never returns the original query as a fallback.

PIVOT with values discovered from data is rejected because column discovery requires executing auxiliary statements. Specify the values with an explicit IN list to export supported PIVOT queries. Unresolved parameters and explicit optimizer opt-outs may also leave unsupported plan nodes.

## Execution and effects

Explaining does not execute the query or its generated replacement. Ordinary binding and optimization still occur, including catalog lookup and source schema discovery. Representable runtime or effecting calls may appear in the returned SQL:

```sql
CREATE SEQUENCE example_sequence;
EXPLAIN (SQL) SELECT nextval('example_sequence');
```

Explanation does not advance the sequence. Executing the returned query advances it once. Queries that cannot preserve observable evaluation behavior are rejected explicitly. Source and extension export callbacks must return owned SQL representations without executing their source or performing effects.

The debug SQL-export verifier is a separate testing facility. `EXPLAIN (SQL)` works without enabling it and does not run differential executions to establish correctness.
