WITH RECURSIVE
directions(delta_x, delta_y) AS (
	VALUES
	    (1::SMALLINT, 0::SMALLINT),
	    (0::SMALLINT, 1::SMALLINT),
	    (-1::SMALLINT, 0::SMALLINT),
	    (0::SMALLINT, -1::SMALLINT)
),
maximum_steps(value) AS (
	SELECT count(*)
	FROM aoc2024_day20_field
	WHERE value <> '#'
),
from_start(step, x, y, last_x, last_y) AS (
	SELECT 0::SMALLINT, x, y, -1::SMALLINT, -1::SMALLINT
	FROM aoc2024_day20_field
	WHERE value = 'S'

	UNION ALL

	SELECT DISTINCT
	       (current.step + 1)::SMALLINT,
	       next.x,
	       next.y,
	       current.x,
	       current.y
	FROM from_start current
	CROSS JOIN directions
	JOIN aoc2024_day20_field next
	  ON next.x = current.x + directions.delta_x
	 AND next.y = current.y + directions.delta_y
	 AND next.value <> '#'
	CROSS JOIN maximum_steps
	WHERE (next.x <> current.last_x OR next.y <> current.last_y)
	  AND current.step < maximum_steps.value
),
from_end(step, x, y, last_x, last_y) AS (
	SELECT 0::SMALLINT, x, y, -1::SMALLINT, -1::SMALLINT
	FROM aoc2024_day20_field
	WHERE value = 'E'

	UNION ALL

	SELECT DISTINCT
	       (current.step + 1)::SMALLINT,
	       next.x,
	       next.y,
	       current.x,
	       current.y
	FROM from_end current
	CROSS JOIN directions
	JOIN aoc2024_day20_field next
	  ON next.x = current.x + directions.delta_x
	 AND next.y = current.y + directions.delta_y
	 AND next.value <> '#'
	CROSS JOIN maximum_steps
	WHERE (next.x <> current.last_x OR next.y <> current.last_y)
	  AND current.step < maximum_steps.value
),
start_distance(x, y, distance) AS (
	SELECT x, y, min(step)
	FROM from_start
	GROUP BY x, y
),
end_distance(x, y, distance) AS (
	SELECT x, y, min(step)
	FROM from_end
	GROUP BY x, y
)
SELECT (SELECT distance
        FROM start_distance
        JOIN aoc2024_day20_field USING (x, y)
        WHERE value = 'E'),
       (SELECT count(*) FROM start_distance),
       (SELECT sum(distance) FROM start_distance),
       (SELECT sum(distance) FROM end_distance),
       md5(
           (SELECT string_agg(x || ':' || y || ':' || distance, ',' ORDER BY x, y)
            FROM start_distance)
       );
