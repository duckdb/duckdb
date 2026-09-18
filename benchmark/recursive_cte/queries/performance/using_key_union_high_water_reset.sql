WITH RECURSIVE t(k, v) USING KEY (k, max(v)) AS (
	SELECT i, 0::BIGINT FROM range(1, 250001) r(i)
	UNION
	SELECT k, v + CASE WHEN duplicate_id = 0 THEN 1 ELSE 0 END
	FROM (SELECT k, max(v) AS v FROM t GROUP BY k) frontier, range(16) duplicates(duplicate_id)
	WHERE (v = 0 OR k = 1) AND v < 5001
)
SELECT count(*) AS row_count, sum(v)::BIGINT AS value_sum, max(v) AS maximum_value
FROM t;
