WITH RECURSIVE
evaluate(id, value) AS (
	SELECT id, value
	FROM aoc2022_day21_monkeys
	WHERE value IS NOT NULL

	UNION ALL

	SELECT parent.id,
	       CASE parent.id % 3
	           WHEN 0 THEN left_value.value + right_value.value
	           WHEN 1 THEN left_value.value - right_value.value
	           ELSE left_value.value + right_value.value * 2
	       END
	FROM aoc2022_day21_monkeys parent
	JOIN evaluate left_value ON parent.left_child = left_value.id
	JOIN evaluate right_value ON parent.right_child = right_value.id
),
path_to_leaf(step, id, sibling, value) AS (
	SELECT 0, 0, NULL::INTEGER, (SELECT value FROM evaluate WHERE id = 0)

	UNION ALL

	SELECT path.step + 1,
	       parent.left_child,
	       parent.right_child,
	       sibling_value.value
	FROM path_to_leaf path
	JOIN aoc2022_day21_monkeys parent ON path.id = parent.id
	JOIN evaluate sibling_value ON parent.right_child = sibling_value.id
	WHERE parent.left_child IS NOT NULL
)
SELECT (SELECT value FROM evaluate WHERE id = 0),
       (SELECT count(*) FROM evaluate),
       (SELECT sum(value) FROM evaluate),
       (SELECT max(step) FROM path_to_leaf),
       md5(string_agg(id || ':' || value, ',' ORDER BY step) FILTER (WHERE sibling IS NOT NULL))
FROM path_to_leaf;
