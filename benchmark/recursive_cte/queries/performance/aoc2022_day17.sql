WITH RECURSIVE
pyroclastic(flow) AS (
	SELECT {
	    shape: 1,
	    jet: 0,
	    jet_count: (SELECT count(*) FROM aoc2022_day17_jets),
	    rock: (SELECT bits FROM aoc2022_day17_rocks WHERE id = 0),
	    y: 1,
	    chamber: [
	        aoc2022_day17_bits('#.......#'),
	        aoc2022_day17_bits('#.......#'),
	        aoc2022_day17_bits('#.......#'),
	        aoc2022_day17_bits('#.......#'),
	        aoc2022_day17_bits('#########')
	    ]
	}

	UNION ALL

	SELECT CASE
	    WHEN aoc2022_day17_collide(pushed_rock, flow.chamber[flow.y + 1:])
	        THEN {
	            shape: flow.shape + 1,
	            jet: (flow.jet + 1) % flow.jet_count,
	            jet_count: flow.jet_count,
	            rock: next_rock,
	            y: 1,
	            chamber:
	                [aoc2022_day17_bits('#.......#') FOR row IN next_rock || [1, 2, 3]]
	                || [row FOR row IN flow.chamber[1:flow.y - 1]
	                    IF row > aoc2022_day17_bits('#.......#')]
	                || aoc2022_day17_merge(pushed_rock, flow.chamber[flow.y:])
	                || flow.chamber[flow.y + length(pushed_rock):]
	        }
	    ELSE {
	        shape: flow.shape,
	        jet: (flow.jet + 1) % flow.jet_count,
	        jet_count: flow.jet_count,
	        rock: pushed_rock,
	        y: flow.y + 1,
	        chamber: flow.chamber
	    }
	END
	FROM (
		SELECT flow,
		       CASE jets.jet
		           WHEN '<' THEN aoc2022_day17_push_left(flow.rock, flow.chamber[flow.y:])
		           WHEN '>' THEN aoc2022_day17_push_right(flow.rock, flow.chamber[flow.y:])
		       END AS pushed_rock,
		       rocks.bits AS next_rock
		FROM pyroclastic,
		     aoc2022_day17_jets jets,
		     aoc2022_day17_rocks rocks
		WHERE flow.jet = jets.id
		  AND flow.shape % 5 = rocks.id
	)
	WHERE flow.shape <= aoc2022_day17_rock_count()
)
SELECT flow.shape,
       length([1 FOR row IN flow.chamber IF row > aoc2022_day17_bits('#.......#')]) - 1,
       flow.jet,
       md5(flow.chamber::VARCHAR)
FROM pyroclastic
WHERE flow.shape > aoc2022_day17_rock_count();
