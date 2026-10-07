WITH RECURSIVE
nodes(node) AS (
	SELECT i::INTEGER FROM range(1, 41) r(i)
),
edges(src, dst, weight) AS (
	SELECT src.node, dst.node, 1 + ((src.node * 29 + dst.node * 43) % 10)
	FROM nodes src, nodes dst
	WHERE src.node != dst.node
	  AND ((src.node * 17 + dst.node * 31) % 100) < 50
),
numbered_edges(id, src, dst, weight) AS (
	SELECT row_number() OVER (ORDER BY weight, src, dst), src, dst, weight
	FROM edges
),
kruskal(node, comp, selected_edges, weight) USING KEY (node) AS (
	SELECT node, node, []::INTEGER[2][], 0 FROM nodes
	UNION ALL
	SELECT current.node, update_row.comp, update_row.selected_edges, update_row.weight
	FROM (
		SELECT left_state.node, left_state.comp,
			left_state.selected_edges || right_state.selected_edges || [[edge.src, edge.dst]],
			left_state.weight + right_state.weight + edge.weight,
			right_state.comp AS old_component
		FROM recurring.kruskal left_state,
		     recurring.kruskal right_state,
		     numbered_edges edge
		WHERE left_state.node = edge.src
		  AND right_state.node = edge.dst
		  AND left_state.comp != right_state.comp
		  AND left_state.comp < right_state.comp
		ORDER BY edge.id
		LIMIT 1
	) update_row(node, comp, selected_edges, weight, old_component),
	  recurring.kruskal current
	WHERE update_row.node = current.node
	   OR update_row.old_component = current.comp
	   OR update_row.comp = current.comp
),
components AS (
	SELECT comp, selected_edges, weight
	FROM kruskal
	GROUP BY comp, selected_edges, weight
)
SELECT count(*) AS components, min(comp) AS minimum_component,
	max(len(selected_edges)) AS selected_edges, max(weight) AS total_weight
FROM components;
