WITH RECURSIVE
edges(src, dst) AS (
	VALUES ('a', 'b'), ('b', 'd'), ('b', 'e'), ('e', 'h'), ('a', 'c'), ('c', 'f'), ('c', 'g')
),
bfs(node, path) USING KEY (node) AS (
	SELECT 'a', 'a'
	UNION ALL
	SELECT e.dst, bfs.path || '->' || e.dst
	FROM bfs
	JOIN edges e ON bfs.node = e.src
	LEFT JOIN recurring.bfs rec ON e.dst = rec.node
	WHERE rec.node IS NULL
)
SELECT count(*) AS nodes, max(length(path)) AS longest_path,
	string_agg(node, ',' ORDER BY node) AS visited
FROM bfs;
