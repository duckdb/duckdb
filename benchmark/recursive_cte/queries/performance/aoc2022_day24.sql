WITH RECURSIVE
blizzard(minute, id, x, y, dx, dy) AS (
	SELECT 0, id, x, y, dx, dy
	FROM aoc2022_day24_blizzards

	UNION ALL

	SELECT minute + 1,
	       id,
	       aoc2022_day24_modulo(x + dx, width),
	       aoc2022_day24_modulo(y + dy, height),
	       dx,
	       dy
	FROM blizzard, aoc2022_day24_config
	WHERE minute + 1 < repeat
),
blocked(minute, x, y) AS (
	SELECT DISTINCT minute, x, y
	FROM blizzard
),
forward(minute, x, y, done) AS (
	SELECT 0, 0, -1, false

	UNION ALL

	SELECT DISTINCT
	       next_minute,
	       candidate.x,
	       candidate.y,
	       bool_or(candidate.x = width - 1 AND candidate.y = height) OVER ()
	FROM forward,
	     aoc2022_day24_config,
	     LATERAL (SELECT minute + 1 AS next_minute),
	     LATERAL (
	         VALUES (x, y), (x - 1, y), (x + 1, y), (x, y - 1), (x, y + 1)
	     ) candidate(x, y)
	WHERE NOT done
	  AND (
	      (candidate.x = 0 AND candidate.y = -1)
	      OR (candidate.x = width - 1 AND candidate.y = height)
	      OR (
	          candidate.x >= 0 AND candidate.x < width
	          AND candidate.y >= 0 AND candidate.y < height
	          AND NOT EXISTS (
	              SELECT 1
	              FROM blocked
	              WHERE blocked.minute = next_minute % repeat
	                AND blocked.x = candidate.x
	                AND blocked.y = candidate.y
	          )
	      )
	  )
),
backward(minute, x, y, done) AS (
	SELECT min(minute), width - 1, height, false
	FROM forward, aoc2022_day24_config
	WHERE done
	GROUP BY width, height

	UNION ALL

	SELECT DISTINCT
	       next_minute,
	       candidate.x,
	       candidate.y,
	       bool_or(candidate.x = 0 AND candidate.y = -1) OVER ()
	FROM backward,
	     aoc2022_day24_config,
	     LATERAL (SELECT minute + 1 AS next_minute),
	     LATERAL (
	         VALUES (x, y), (x - 1, y), (x + 1, y), (x, y - 1), (x, y + 1)
	     ) candidate(x, y)
	WHERE NOT done
	  AND (
	      (candidate.x = 0 AND candidate.y = -1)
	      OR (candidate.x = width - 1 AND candidate.y = height)
	      OR (
	          candidate.x >= 0 AND candidate.x < width
	          AND candidate.y >= 0 AND candidate.y < height
	          AND NOT EXISTS (
	              SELECT 1
	              FROM blocked
	              WHERE blocked.minute = next_minute % repeat
	                AND blocked.x = candidate.x
	                AND blocked.y = candidate.y
	          )
	      )
	  )
),
forward_again(minute, x, y, done) AS (
	SELECT min(minute), 0, -1, false
	FROM backward
	WHERE done

	UNION ALL

	SELECT DISTINCT
	       next_minute,
	       candidate.x,
	       candidate.y,
	       bool_or(candidate.x = width - 1 AND candidate.y = height) OVER ()
	FROM forward_again,
	     aoc2022_day24_config,
	     LATERAL (SELECT minute + 1 AS next_minute),
	     LATERAL (
	         VALUES (x, y), (x - 1, y), (x + 1, y), (x, y - 1), (x, y + 1)
	     ) candidate(x, y)
	WHERE NOT done
	  AND (
	      (candidate.x = 0 AND candidate.y = -1)
	      OR (candidate.x = width - 1 AND candidate.y = height)
	      OR (
	          candidate.x >= 0 AND candidate.x < width
	          AND candidate.y >= 0 AND candidate.y < height
	          AND NOT EXISTS (
	              SELECT 1
	              FROM blocked
	              WHERE blocked.minute = next_minute % repeat
	                AND blocked.x = candidate.x
	                AND blocked.y = candidate.y
	          )
	      )
	  )
)
SELECT (SELECT min(minute) FROM forward WHERE done),
       (SELECT min(minute) FROM backward WHERE done),
       (SELECT min(minute) FROM forward_again WHERE done),
       (SELECT count(*) FROM forward),
       (SELECT count(*) FROM backward),
       (SELECT count(*) FROM forward_again);
