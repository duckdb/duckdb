-- Issue #21836: OOM / excessive memory on INSERT ... ON CONFLICT with large fixed arrays.
-- Adjust N (array length) and memory_limit to stress-test.

SET memory_limit = '120MB';
SET threads = 1;

CREATE TABLE t (id INT PRIMARY KEY, a DOUBLE[128], b DOUBLE[128], c DOUBLE[128]);

INSERT INTO t (id, a)
VALUES (1, (SELECT list(0.0) FROM generate_series(1, 128))::DOUBLE[128]);

INSERT INTO t (id, b)
VALUES (1, (SELECT list(1.0) FROM generate_series(1, 128))::DOUBLE[128])
ON CONFLICT DO UPDATE SET b = EXCLUDED.b, a = a, c = c;

SELECT id, a[1], b[1], c[1] FROM t;
