WITH RECURSIVE
rounds(round, directions, xy, stable) AS (
	SELECT 0,
	       aoc2022_day23_directions(),
	       scan.xy,
	       false
	FROM aoc2022_day23_scan scan

	UNION ALL

	SELECT proposal.round + 1,
	       aoc2022_day23_rotate(proposal.directions),
	       CASE
	           WHEN count(*) OVER (PARTITION BY proposal.move) = 1 THEN proposal.move
	           ELSE proposal.xy
	       END,
	       bool_and(proposal.move = proposal.xy) OVER ()
	FROM (
		SELECT current.*,
		       CASE
		           WHEN vicinity
		               THEN aoc2022_day23_add(
		                   current.xy,
		                   aoc2022_day23_propose(vicinity, current.directions)
		               )
		           ELSE current.xy
		       END AS move
		FROM rounds current,
		LATERAL (
			SELECT bit_or(aoc2022_day23_direction(delta_x, delta_y))
			FROM rounds neighbor,
			     generate_series(-1, 1) x_offsets(delta_x),
			     generate_series(-1, 1) y_offsets(delta_y)
			WHERE aoc2022_day23_add(
			    current.xy,
			    aoc2022_day23_v2(delta_x, delta_y)
			) = neighbor.xy
		) neighborhood(vicinity)
	) proposal
	WHERE NOT proposal.stable
),
final_round AS (
	SELECT *
	FROM rounds
	WHERE stable
)
SELECT max(round) AS stable_round,
       count(*) AS elves,
       (max(xy.x) - min(xy.x) + 1) * (max(xy.y) - min(xy.y) + 1) - count(*) AS empty_ground,
       md5(string_agg(xy.x || ',' || xy.y, ';' ORDER BY xy.x, xy.y)) AS position_checksum
FROM final_round;
