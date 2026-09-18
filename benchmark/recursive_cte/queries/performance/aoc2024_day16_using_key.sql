WITH RECURSIVE
directions(direction, delta_x, delta_y) AS (
	VALUES
	    (0, 1::SMALLINT, 0::SMALLINT),
	    (1, 0::SMALLINT, 1::SMALLINT),
	    (2, -1::SMALLINT, 0::SMALLINT),
	    (3, 0::SMALLINT, -1::SMALLINT)
),
turns(turn, cost) AS (
	VALUES (0, 0), (1, 1000), (2, 2000), (3, 1000)
),
best(x, y, direction, cost) USING KEY (x, y, direction) AS (
	SELECT x, y, 0, 0
	FROM aoc2024_day16_field
	WHERE value = 'S'

	UNION ALL

	(
		WITH candidates AS (
			SELECT next.x,
			       next.y,
			       (current.direction + turns.turn) % 4 AS direction,
			       current.cost + turns.cost + 1 AS cost
			FROM best current
			CROSS JOIN turns
			JOIN directions
			  ON directions.direction = (current.direction + turns.turn) % 4
			JOIN aoc2024_day16_field next
			  ON next.x = current.x + directions.delta_x
			 AND next.y = current.y + directions.delta_y
			 AND next.value <> '#'
		),
		cheapest AS (
			SELECT x, y, direction, min(cost) AS cost
			FROM candidates
			GROUP BY x, y, direction
		)
		SELECT cheapest.*
		FROM cheapest
		LEFT JOIN recurring.best previous USING (x, y, direction)
		WHERE previous.x IS NULL OR cheapest.cost < previous.cost
	)
),
oriented(x, y, direction, cost) AS (
	SELECT x, y, direction, min(cost)
	FROM (
		SELECT x,
		       y,
		       (direction + turns.turn) % 4 AS direction,
		       best.cost + turns.cost AS cost
		FROM best
		CROSS JOIN turns
	)
	GROUP BY x, y, direction
),
best_cost(cost) AS (
	SELECT min(cost)
	FROM oriented
	JOIN aoc2024_day16_field USING (x, y)
	WHERE value = 'E'
),
best_path(x, y, direction, cost) AS (
	SELECT oriented.*
	FROM oriented
	JOIN aoc2024_day16_field USING (x, y)
	JOIN best_cost USING (cost)
	WHERE value = 'E'

	UNION ALL

	(
		WITH current AS (
			SELECT DISTINCT *
			FROM best_path
		)
		SELECT predecessor.x,
		       predecessor.y,
		       predecessor.direction,
		       predecessor.cost
		FROM current
		CROSS JOIN turns
		JOIN oriented predecessor
		  ON predecessor.x = current.x
		 AND predecessor.y = current.y
		 AND predecessor.direction = (current.direction + 4 - turns.turn) % 4
		 AND predecessor.cost = current.cost - turns.cost
		WHERE turns.turn > 0

		UNION ALL

		SELECT predecessor.x,
		       predecessor.y,
		       predecessor.direction,
		       predecessor.cost
		FROM current
		JOIN directions ON directions.direction = current.direction
		JOIN oriented predecessor
		  ON predecessor.x = current.x - directions.delta_x
		 AND predecessor.y = current.y - directions.delta_y
		 AND predecessor.direction = current.direction
		 AND predecessor.cost = current.cost - 1
	)
)
SELECT (SELECT cost FROM best_cost),
       count(DISTINCT (x, y))
FROM best_path;
