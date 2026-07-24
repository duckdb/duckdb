WITH RECURSIVE
params(node_count) AS (
	VALUES (200)
),
nodes(node) AS (
	SELECT node::INTEGER
	FROM params, range(node_count) nodes(node)
),
edges(here, there) AS (
	SELECT node, node + 1 FROM nodes, params WHERE node + 1 < node_count
	UNION ALL
	SELECT node + 1, node FROM nodes, params WHERE node + 1 < node_count
),
cc(node, comp) USING KEY (node) AS (
	SELECT node, node FROM nodes
	UNION ALL
	(
		SELECT current.node, changed.comp
		FROM cc changed, edges edge, recurring.cc current
		WHERE (edge.here, edge.there) = (current.node, changed.node)
		  AND changed.comp < current.comp
		ORDER BY current.node, changed.comp
	)
)
SELECT count(*) AS nodes, count(DISTINCT comp) AS components,
	sum(comp)::BIGINT AS component_checksum
FROM cc;
