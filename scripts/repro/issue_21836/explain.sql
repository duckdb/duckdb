-- Plan shape for INSERT ... ON CONFLICT (rewritten to MERGE + DISTINCT ON).
-- Look for HASH_GROUP_BY and first(...) aggregates over array-typed columns.

CREATE TABLE t (id INT PRIMARY KEY, a DOUBLE[32], b DOUBLE[32], c DOUBLE[32]);

INSERT INTO t (id, a)
VALUES (1, (SELECT list(0.0) FROM generate_series(1, 32))::DOUBLE[32]);

EXPLAIN INSERT INTO t (id, b)
VALUES (1, (SELECT list(1.0) FROM generate_series(1, 32))::DOUBLE[32])
ON CONFLICT DO UPDATE SET b = EXCLUDED.b, a = a, c = c;
