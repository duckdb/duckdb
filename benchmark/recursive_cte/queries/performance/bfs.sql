WITH RECURSIVE
params(node_count) AS (
	VALUES (1000000)
),
edges(src, dst) AS (
	SELECT node::BIGINT, (node * 2 + 1)::BIGINT
	FROM params, range(node_count) nodes(node)
	WHERE node * 2 + 1 < node_count
	UNION ALL
	SELECT node::BIGINT, (node * 2 + 2)::BIGINT
	FROM params, range(node_count) nodes(node)
	WHERE node * 2 + 2 < node_count
),
bfs(node, distance) USING KEY (node) AS (
	VALUES (0::BIGINT, 0::INTEGER)
	UNION ALL
	SELECT edge.dst, frontier.distance + 1
	FROM bfs frontier
	JOIN edges edge ON frontier.node = edge.src
	LEFT JOIN recurring.bfs state ON edge.dst = state.node
	WHERE state.node IS NULL
)
SELECT count(*) AS nodes, max(distance) AS maximum_distance,
	sum(distance)::BIGINT AS distance_checksum
FROM bfs;
