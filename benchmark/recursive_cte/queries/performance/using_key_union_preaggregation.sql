WITH RECURSIVE t(k, v) USING KEY (k, min(v)) AS (
	SELECT i, 4::BIGINT FROM range(50000) r(i), range(8) duplicates(_)
	UNION
	SELECT k, v - 1 FROM t, range(8) duplicates(_) WHERE v > 0
)
SELECT count(*) AS row_count, sum(v)::BIGINT AS value_sum, max(v) AS maximum_value
FROM t;
