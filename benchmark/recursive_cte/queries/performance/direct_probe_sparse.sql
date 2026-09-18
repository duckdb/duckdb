WITH RECURSIVE state(key, value) USING KEY (key) AS (
	SELECT key, CASE WHEN key < 1000 THEN 0 ELSE 100 END
	FROM range(1000000) keys(key)
	UNION ALL
	SELECT frontier.key, recurring_state.value + 1
	FROM state frontier
	JOIN recurring.state recurring_state USING (key)
	WHERE frontier.value < 20
)
SELECT count(*) AS keys, sum(value)::BIGINT AS value_sum
FROM state;
