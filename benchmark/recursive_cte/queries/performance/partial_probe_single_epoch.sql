WITH RECURSIVE state(bucket, item, level) USING KEY (bucket, item) AS (
	SELECT bucket, item, CASE WHEN bucket = 0 THEN 0 ELSE 100 END
	FROM range(1000) buckets(bucket), range(100) items(item)
	UNION ALL
	SELECT frontier.bucket + 1, recurring_state.item, frontier.level + 1
	FROM state frontier
	JOIN recurring.state recurring_state ON recurring_state.bucket = frontier.bucket
	WHERE frontier.item = 0 AND frontier.level < 1
)
SELECT count(*) AS keys, sum(level)::BIGINT AS level_sum, max(level) AS max_level
FROM state;
