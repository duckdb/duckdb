CREATE TABLE t1 (g INT, s VARCHAR);
INSERT INTO t1 VALUES (1, 'a'), (1, 'a'), (2, 'c');
SELECT g, string_agg(s, '-' ORDER BY s), string_agg(DISTINCT s, '/'), string_agg(s) FROM t1 GROUP BY g ORDER BY g;
