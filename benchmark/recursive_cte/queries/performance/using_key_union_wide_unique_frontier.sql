WITH RECURSIVE t(k, v) USING KEY (k, max(v)) AS (
	SELECT repeat(md5(i::VARCHAR), 32), 0::BIGINT FROM range(2048) r(i)
	UNION
	SELECT k, v + 1 FROM t WHERE v < 50
)
SELECT count(*) AS row_count, sum(v)::BIGINT AS value_sum, max(v) AS maximum_value
FROM t;
