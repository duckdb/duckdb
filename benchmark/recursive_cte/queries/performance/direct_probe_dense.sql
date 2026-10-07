WITH RECURSIVE state(key, value) USING KEY (key) AS (
	SELECT key, 0 FROM range(1000000) keys(key)
	UNION ALL
	SELECT frontier.key, recurring_state.value + 1
	FROM state frontier
	JOIN recurring.state recurring_state USING (key)
	WHERE frontier.value < 5
)
SELECT count(*) AS keys, sum(value)::BIGINT AS value_sum
FROM state;
