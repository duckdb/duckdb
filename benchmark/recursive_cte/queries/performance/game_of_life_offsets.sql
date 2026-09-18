WITH RECURSIVE
params(width, height, iterations) AS (
	VALUES (21, 21, 200)
),
offsets(dx, dy) AS (
	VALUES (-1, -1), (-1, 0), (-1, 1), (0, -1),
	       (0, 1), (1, -1), (1, 0), (1, 1)
),
initial(x, y, val) AS (
	SELECT x, y, (((x * 17 + y * 31 + x * y * 7) % 11) < 5)::INTEGER
	FROM params, range(21) xs(x), range(21) ys(y)
),
game_of_life(iter, x, y, val) USING KEY (x, y) AS (
	SELECT 0, x, y, val FROM initial
	UNION ALL
	SELECT candidate.iter + 1, candidate.x, candidate.y,
		CASE sum(neighbor.val) WHEN 2 THEN candidate.val WHEN 3 THEN 1 ELSE 0 END
	FROM (
		SELECT cell.iter, cell.x, cell.y, cell.val,
			cell.x + offsets.dx AS neighbor_x,
			cell.y + offsets.dy AS neighbor_y
		FROM recurring.game_of_life cell
		CROSS JOIN offsets
		CROSS JOIN params
		WHERE cell.iter < iterations
		  AND cell.x + offsets.dx BETWEEN 0 AND width - 1
		  AND cell.y + offsets.dy BETWEEN 0 AND height - 1
	) candidate
	JOIN recurring.game_of_life neighbor
	  ON neighbor.x = candidate.neighbor_x
	 AND neighbor.y = candidate.neighbor_y
	GROUP BY candidate.iter, candidate.x, candidate.y, candidate.val
)
SELECT max(iter) AS iterations, count(*) AS cells, sum(val)::BIGINT AS live_cells
FROM game_of_life;
