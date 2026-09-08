WITH RECURSIVE t(i) AS (
	VALUES (0::BIGINT)
	UNION ALL
	SELECT i + 1 FROM t WHERE i < 49999
)
SELECT count(*) AS row_count, sum(i)::BIGINT AS value_sum, max(i) AS max_value
FROM t;
