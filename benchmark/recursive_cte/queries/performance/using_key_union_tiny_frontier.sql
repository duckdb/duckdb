WITH RECURSIVE t(k, v) USING KEY (k, min(v)) AS (
	VALUES (0::BIGINT, 0::BIGINT)
	UNION
	SELECT k + 1, v + 1 FROM t WHERE k < 20000
)
SELECT count(*) AS row_count, sum(v)::BIGINT AS value_sum, max(v) AS maximum_value
FROM t;
