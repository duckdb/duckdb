WITH RECURSIVE k_core(node, deg, active) USING KEY (node) AS (
	SELECT v.node, count(e."to") AS deg, deg >= 2 AS active
	FROM nodes AS v
	LEFT JOIN edges AS e ON v.node = e."from"
	GROUP BY v.node

	UNION ALL

	SELECT v.node, countif(n.active) AS degree, degree >= 2 AS active
	FROM k_core AS v, edges AS e, recurring.k_core AS n
	WHERE (v.node, n.node) = (e."from", e."to")
	GROUP BY ALL
	HAVING degree <> v.deg
)
SELECT count(*) FILTER (active) AS active_nodes,
       min(node) FILTER (active) AS minimum,
       max(node) FILTER (active) AS maximum,
       sum(node) FILTER (active)::HUGEINT AS total
FROM k_core;
