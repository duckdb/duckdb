WITH RECURSIVE
nodes(node) AS (
	VALUES (0), (1), (2), (3), (4), (5), (6)
),
edges(here, there) AS (
	VALUES (0, 4), (4, 0), (0, 3), (3, 0), (1, 4), (4, 1),
	       (3, 4), (4, 3), (2, 5), (5, 2)
),
cc(node, comp) USING KEY (node) AS (
	SELECT node, node FROM nodes
	UNION ALL
	(
		SELECT current.node, changed.comp
		FROM cc changed, edges e, recurring.cc current
		WHERE (e.here, e.there) = (current.node, changed.node)
		  AND changed.comp < current.comp
		ORDER BY current.node, changed.comp
	)
)
SELECT count(*) AS nodes, count(DISTINCT comp) AS components,
	sum(comp)::BIGINT AS component_checksum
FROM cc;
