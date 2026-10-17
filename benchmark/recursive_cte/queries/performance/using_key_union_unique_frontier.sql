WITH RECURSIVE t(k, v) USING KEY (k, min(v)) AS (
	SELECT i, 0::BIGINT FROM range(500000) r(i)
	UNION
	SELECT k + 500000, v + 1 FROM t WHERE v < 2
)
SELECT count(*) AS row_count, sum(v)::BIGINT AS value_sum, max(v) AS maximum_value
FROM t;
