WITH RECURSIVE
heightmap(row, col, height) AS (
	SELECT row,
	       col,
	       CASE
	           WHEN row = 128 AND col = 128 THEN 'E'
	           ELSE chr(greatest(
	               ascii('a'),
	               ascii('z') - least((abs(row - 128) + abs(col - 128)) // 4, 25)
	           )::INTEGER)
	       END
	FROM range(256) rows(row), range(256) columns(col)
),
path_finding(row, col, height, steps, visited) AS (
	SELECT row, col, height, 0, [[row, col]]
	FROM heightmap
	WHERE height = 'E'

	UNION ALL

	(
		WITH visited_positions AS (
			SELECT DISTINCT unnest(visited) AS position
			FROM path_finding
		)
		SELECT DISTINCT ON (next.row, next.col, next.height, current.steps)
		       next.row,
		       next.col,
		       next.height,
		       current.steps + 1,
		       array_append(current.visited, [next.row, next.col])
		FROM (VALUES (-1, 0), (1, 0), (0, 1), (0, -1)) direction(row, col)
		JOIN path_finding current ON true
		JOIN heightmap next
		  ON current.row + direction.row = next.row
		 AND current.col + direction.col = next.col
		WHERE NOT array_contains(current.visited, [next.row, next.col])
		  AND aoc2022_day12_height(current.height) <= aoc2022_day12_height(next.height) + 1
		  AND current.height <> 'a'
		  AND [next.row, next.col] NOT IN (
		      SELECT position FROM visited_positions
		  )
	)
)
SELECT min(steps) AS shortest_path,
       count(*) AS reachable_low_points,
       sum(row) AS row_sum,
       sum(col) AS col_sum
FROM path_finding
WHERE height = 'a';
