CREATE TABLE t1 (g INT, v INTEGER, d DECIMAL(10,3));
INSERT INTO t1 VALUES (1, 1, 1.5), (1, 2, 2.5), (1, 3, 3.5), (2, 10, 10.5);
SELECT g, reservoir_quantile(v, 0.5), reservoir_quantile(v, 0.5, 1024), reservoir_quantile(v, [0.25, 0.75]), CAST(reservoir_quantile(d, 0.5, 512) AS BIGINT) FROM t1 GROUP BY g ORDER BY g;
