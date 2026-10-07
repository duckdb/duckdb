CREATE MACRO aoc2022_day09_follow(t, delta_x, delta_y) AS
	CASE
		WHEN abs(delta_x) > 1 OR abs(delta_y) > 1
			THEN {x: t.x + sign(delta_x), y: t.y + sign(delta_y)}
		ELSE t
	END;

CREATE TABLE aoc2022_day09_head AS
WITH steps(move, delta_x, delta_y) AS (
	SELECT move,
	       CASE direction WHEN 0 THEN 1 WHEN 2 THEN -1 ELSE 0 END AS delta_x,
	       CASE direction WHEN 1 THEN 1 WHEN 3 THEN -1 ELSE 0 END AS delta_y
	FROM (
		SELECT move,
		       ((move // 29) + (move // 113)) % 4 AS direction
		FROM range(1, 4097) moves(move)
	)
)
SELECT move,
       {
           x: sum(delta_x) OVER (ORDER BY move),
           y: sum(delta_y) OVER (ORDER BY move)
       } AS xy
FROM steps;
