CREATE TABLE t1 (g INT, s VARCHAR);
INSERT INTO t1 VALUES (1, 'a'), (1, 'b'), (2, 'c');
SELECT g, s, string_agg(s, '-') OVER (PARTITION BY g) FROM t1 ORDER BY g, s;
