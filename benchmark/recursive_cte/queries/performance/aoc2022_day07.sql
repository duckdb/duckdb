WITH RECURSIVE
ascent(node, file, size) AS (
	SELECT node, node AS file, size
	FROM aoc2022_day07_tree
	WHERE NOT is_directory

	UNION ALL

	SELECT tree.parent, ascent.file, ascent.size
	FROM ascent
	JOIN aoc2022_day07_tree tree USING (node)
	WHERE tree.parent IS NOT NULL
),
sizes(node, size) AS (
	SELECT node, sum(size)
	FROM ascent
	GROUP BY node
)
SELECT count(*),
       max(size) FILTER (WHERE node = 0),
       sum(size),
       md5(string_agg(node || ':' || size, ',' ORDER BY node))
FROM sizes;
