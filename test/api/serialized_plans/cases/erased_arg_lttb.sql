CREATE TABLE t1 (g INT, x DOUBLE, y DOUBLE);
INSERT INTO t1 VALUES (1, 1, 1), (1, 2, 5), (1, 3, 2), (1, 4, 9), (1, 5, 3), (2, 1, 1), (2, 2, 2);
SELECT g, lttb(x, y, 3) FROM t1 GROUP BY g ORDER BY g;
