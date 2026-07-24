WITH RECURSIVE
edges(here, there) AS (
	VALUES (0, 3), (0, 1), (1, 0), (1, 2), (3, 4), (2, 3)
),
nodes(node) AS (
	SELECT here FROM edges UNION SELECT there FROM edges
),
edge_degrees(here, there, outgoing) AS (
	SELECT here, there, count(*) OVER (PARTITION BY here) FROM edges
),
node_count(n) AS (
	SELECT count(*) FROM nodes
),
pagerank(node, rank, iter) USING KEY (node) AS (
	SELECT node, 1.0 / n, 0 FROM nodes, node_count
	UNION ALL
	SELECT contribution.node,
		((1 - 0.9) / n) + 0.9 * sum(contribution.rank), contribution.iter + 1
	FROM (
		SELECT edge.there AS node, current.rank / edge.outgoing AS rank, current.iter
		FROM recurring.pagerank current
		JOIN edge_degrees edge ON current.node = edge.here
	) contribution, node_count
	WHERE contribution.iter < 100
	GROUP BY contribution.node, contribution.iter, n
)
SELECT count(*) AS nodes, max(iter) AS iterations,
	round(sum(rank), 12) AS rank_sum,
	round(sum(node * rank), 12) AS rank_checksum
FROM pagerank;
