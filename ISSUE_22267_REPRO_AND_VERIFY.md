# Issue #22267 Repro and Fix Verification

## Description

Fixes a correlated `EXISTS` correctness bug where optimization could mis-rewrite correlated refs in a derived-table path, producing wrong results (issue [#22267](https://github.com/duckdb/duckdb/issues/22267)).

- Add missing correlated-expression rewrite in `MARK`-join decorrelation path.
- Add regression test `test/sql/subquery/exists/test_issue_22267.test` with repro + variant.

## 1. Reproduce the Original Issue

Use DuckDB CLI (or shell mode) and run:

```sql
CREATE TABLE posts (
	id INTEGER,
	user_id INTEGER,
	title VARCHAR,
	content VARCHAR,
	views INTEGER,
	likes INTEGER,
	created_at TIMESTAMP,
	rating DOUBLE
);

INSERT INTO posts VALUES
	(1, 1, 'Hello World', 'First post', 100, 10, '2022-01-10 10:00:00', 4.5),
	(2, 1, 'Another Post', NULL,        150, 20, '2022-01-11 11:00:00', 3.0),
	(3, 2, 'Bob Post',     'Content',   NULL,  5, '2022-01-12 12:00:00', NULL),
	(4, 3, NULL,           'Empty',      50,  2, '2022-01-13 13:00:00', 5.0),
	(5, 4, 'Last Post',    'Last',      300, 30, '2022-01-14 14:00:00', 4.9);

SELECT count(*)
FROM posts AS ref_0
WHERE EXISTS (
	SELECT 1
	FROM (
		SELECT
			ref_0.id AS c0,
			ref_1.views AS c1,
			ref_1.views AS c2
		FROM posts AS ref_1
		WHERE ref_0.likes > ref_1.likes
	) AS subq_0
	WHERE subq_0.c1 != ref_0.likes
);
```

### Expected Behavior
- Result should be `4`.

### Original Buggy Behavior
- With optimizer enabled, result was `0`.
- With optimizer disabled, result was `4`.

To confirm the optimizer-dependent mismatch:

```sql
PRAGMA disable_optimizer;

SELECT count(*)
FROM posts AS ref_0
WHERE EXISTS (
	SELECT 1
	FROM (
		SELECT
			ref_0.id AS c0,
			ref_1.views AS c1,
			ref_1.views AS c2
		FROM posts AS ref_1
		WHERE ref_0.likes > ref_1.likes
	) AS subq_0
	WHERE subq_0.c1 != ref_0.likes
);
```

## 2. Verify the Fix

Our change adds a missing correlated-expression rewrite in the `MARK`-join decorrelation path:

- `src/planner/subquery/flatten_dependent_join.cpp`

and adds regression coverage:

- `test/sql/subquery/exists/test_issue_22267.test`

### A) Rebuild

From repo root:

```bash
make
```

### B) Run Targeted Regression Test

```bash
build/debug/test/unittest --use-colour no test/sql/subquery/exists/test_issue_22267.test
```

### C) Expected Post-Fix Result

- The test passes.
- The repro query returns `4` with optimizer enabled.
- The `PRAGMA disable_optimizer` query also returns `4`.

## 3. Optional Broader Verification

Run fast unit tests:

```bash
make unit
```

If `make`/`make unit` fails due to unrelated compile errors outside this fix, resolve those first and rerun the targeted test above.
