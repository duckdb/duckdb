WITH RECURSIVE
nodes(node) AS (
	SELECT i::INTEGER FROM range(1, 16) r(i)
),
edges(here, there, length) AS (
	SELECT n1.node, n2.node, 1 + ((n1.node * 31 + n2.node * 17) % 10)
	FROM nodes n1, nodes n2
	WHERE n1.node != n2.node
),
bellman(knode, distance) USING KEY (knode) AS (
	SELECT 1, 0
	UNION ALL
	(
		SELECT e.there, n.distance + e.length
		FROM bellman n
		JOIN edges e ON n.knode = e.here
		LEFT JOIN recurring.bellman rec ON e.there = rec.knode
		WHERE n.distance + e.length < coalesce(rec.distance, 'Infinity'::DOUBLE)
		ORDER BY n.distance + e.length DESC
	)
)
SELECT count(*) AS nodes, sum(distance)::BIGINT AS total_distance,
	max(distance)::BIGINT AS maximum_distance
FROM bellman;
