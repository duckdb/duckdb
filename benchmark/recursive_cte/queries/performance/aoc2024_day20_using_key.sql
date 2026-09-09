WITH RECURSIVE
directions(delta_x, delta_y) AS (
	VALUES
	    (1::SMALLINT, 0::SMALLINT),
	    (0::SMALLINT, 1::SMALLINT),
	    (-1::SMALLINT, 0::SMALLINT),
	    (0::SMALLINT, -1::SMALLINT)
),
from_start(x, y, distance) USING KEY (x, y) AS (
	SELECT x, y, 0::SMALLINT
	FROM aoc2024_day20_field
	WHERE value = 'S'

	UNION ALL

	(
		WITH candidates AS (
			SELECT next.x,
			       next.y,
			       (current.distance + 1)::SMALLINT AS distance
			FROM from_start current
			CROSS JOIN directions
			JOIN aoc2024_day20_field next
			  ON next.x = current.x + directions.delta_x
			 AND next.y = current.y + directions.delta_y
			 AND next.value <> '#'
		),
		new_positions AS (
			SELECT candidates.*,
			       row_number() OVER (
			           PARTITION BY candidates.x, candidates.y
			           ORDER BY candidates.distance
			       ) AS candidate_rank
			FROM candidates
			LEFT JOIN recurring.from_start visited USING (x, y)
			WHERE visited.x IS NULL
		)
		SELECT x, y, distance
		FROM new_positions
		WHERE candidate_rank = 1
	)
),
from_end(x, y, distance) USING KEY (x, y) AS (
	SELECT x, y, 0::SMALLINT
	FROM aoc2024_day20_field
	WHERE value = 'E'

	UNION ALL

	(
		WITH candidates AS (
			SELECT next.x,
			       next.y,
			       (current.distance + 1)::SMALLINT AS distance
			FROM from_end current
			CROSS JOIN directions
			JOIN aoc2024_day20_field next
			  ON next.x = current.x + directions.delta_x
			 AND next.y = current.y + directions.delta_y
			 AND next.value <> '#'
		),
		new_positions AS (
			SELECT candidates.*,
			       row_number() OVER (
			           PARTITION BY candidates.x, candidates.y
			           ORDER BY candidates.distance
			       ) AS candidate_rank
			FROM candidates
			LEFT JOIN recurring.from_end visited USING (x, y)
			WHERE visited.x IS NULL
		)
		SELECT x, y, distance
		FROM new_positions
		WHERE candidate_rank = 1
	)
)
SELECT (SELECT distance
        FROM from_start
        JOIN aoc2024_day20_field USING (x, y)
        WHERE value = 'E'),
       (SELECT count(*) FROM from_start),
       (SELECT sum(distance) FROM from_start),
       (SELECT sum(distance) FROM from_end),
       md5(
           (SELECT string_agg(x || ':' || y || ':' || distance, ',' ORDER BY x, y)
            FROM from_start)
       );
