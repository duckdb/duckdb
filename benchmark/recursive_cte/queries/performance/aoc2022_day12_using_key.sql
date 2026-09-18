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
path_finding(row, col, height, steps) USING KEY (row, col) AS (
	SELECT row, col, height, 0
	FROM heightmap
	WHERE height = 'E'

	UNION ALL

	(
		WITH candidates AS (
			SELECT next.row,
			       next.col,
			       next.height,
			       current.steps + 1 AS steps
			FROM (VALUES (-1, 0), (1, 0), (0, 1), (0, -1)) direction(row, col)
			JOIN path_finding current ON true
			JOIN heightmap next
			  ON current.row + direction.row = next.row
			 AND current.col + direction.col = next.col
			WHERE aoc2022_day12_height(current.height) <= aoc2022_day12_height(next.height) + 1
			  AND current.height <> 'a'
		),
		new_positions AS (
			SELECT candidates.*,
			       row_number() OVER (
			           PARTITION BY candidates.row, candidates.col
			           ORDER BY candidates.steps
			       ) AS candidate_rank
			FROM candidates
			LEFT JOIN recurring.path_finding visited
			  ON candidates.row = visited.row
			 AND candidates.col = visited.col
			WHERE visited.row IS NULL
		)
		SELECT row, col, height, steps
		FROM new_positions
		WHERE candidate_rank = 1
	)
)
SELECT min(steps) AS shortest_path,
       count(*) AS reachable_low_points,
       sum(row) AS row_sum,
       sum(col) AS col_sum
FROM path_finding
WHERE height = 'a';
