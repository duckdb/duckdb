CREATE TABLE t1 (g INT, v INTEGER);
INSERT INTO t1 VALUES (1, 1), (1, 3), (1, 5), (2, 10);
SELECT g, bitstring_agg(v, 0, 15) FROM t1 GROUP BY g ORDER BY g;
