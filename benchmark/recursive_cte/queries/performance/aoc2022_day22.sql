WITH RECURSIVE
run(instruction, remaining, x, y, direction, turns) AS (
	SELECT 1,
	       distance,
	       0,
	       0,
	       0,
	       0
	FROM aoc2022_day22_instructions
	WHERE instruction = 1

	UNION ALL

	(
		SELECT instruction,
		       remaining - 1,
		       CASE direction
		           WHEN 0 THEN (x + 1) % aoc2022_day22_width()
		           WHEN 2 THEN (x + aoc2022_day22_width() - 1) % aoc2022_day22_width()
		           ELSE x
		       END,
		       CASE direction
		           WHEN 1 THEN (y + 1) % aoc2022_day22_height()
		           WHEN 3 THEN (y + aoc2022_day22_height() - 1) % aoc2022_day22_height()
		           ELSE y
		       END,
		       direction,
		       turns
		FROM run
		WHERE remaining > 0

		UNION ALL

		SELECT next_instruction.instruction,
		       next_instruction.distance,
		       run.x,
		       run.y,
		       (run.direction + next_instruction.turn + 4) % 4,
		       run.turns + 1
		FROM run
		JOIN aoc2022_day22_instructions next_instruction
		  ON run.instruction + 1 = next_instruction.instruction
		WHERE run.remaining = 0
	)
)
SELECT instruction,
       x,
       y,
       direction,
       turns,
       count(*) OVER ()
FROM run
ORDER BY instruction DESC, remaining
LIMIT 1;
