WITH RECURSIVE
mining(minutes, id, blueprint, resources, robots) AS (
	SELECT 24,
	       input.id,
	       input.blueprint,
	       [0, 0, 0, 0]::aoc2022_day19_vec4,
	       [1, 0, 0, 0]::aoc2022_day19_vec4
	FROM aoc2022_day19_input input

	UNION ALL

	(
		WITH state(
			minutes, id, blueprint, resources, robots,
			wait_time, build_geode_now, build_ore, build_clay, build_obsidian, build_geode
		) AS MATERIALIZED (
			SELECT prepared.*,
			       prepared.wait_time[4] = 1,
			       prepared.wait_time[1] BETWEEN 1 AND prepared.minutes - 1
			           AND prepared.robots[1] < aoc2022_day19_max_minerals(prepared.blueprint, 1),
			       prepared.wait_time[2] BETWEEN 1 AND prepared.minutes - 1
			           AND prepared.robots[2] < aoc2022_day19_max_minerals(prepared.blueprint, 2),
			       prepared.wait_time[3] BETWEEN 1 AND prepared.minutes - 1
			           AND prepared.robots[3] < aoc2022_day19_max_minerals(prepared.blueprint, 3),
			       prepared.wait_time[4] BETWEEN 1 AND prepared.minutes - 1
			           AND prepared.robots[4] < aoc2022_day19_max_minerals(prepared.blueprint, 4)
			FROM (
				SELECT mining.*,
				       [
				           aoc2022_day19_wait(
				               aoc2022_day19_sub(mining.blueprint[1], mining.resources),
				               mining.robots
				           ),
				           aoc2022_day19_wait(
				               aoc2022_day19_sub(mining.blueprint[2], mining.resources),
				               mining.robots
				           ),
				           aoc2022_day19_wait(
				               aoc2022_day19_sub(mining.blueprint[3], mining.resources),
				               mining.robots
				           ),
				           aoc2022_day19_wait(
				               aoc2022_day19_sub(mining.blueprint[4], mining.resources),
				               mining.robots
				           )
				       ] AS wait_time
				FROM mining
				WHERE mining.minutes > 0
			) prepared
		)
		SELECT state.minutes - 1,
		       state.id,
		       state.blueprint,
		       aoc2022_day19_add(aoc2022_day19_sub(state.resources, state.blueprint[4]), state.robots),
		       aoc2022_day19_add(state.robots, [0, 0, 0, 1])
		FROM state
		WHERE state.build_geode_now

		UNION ALL

		SELECT state.minutes - state.wait_time[1],
		       state.id,
		       state.blueprint,
		       aoc2022_day19_add(
		           aoc2022_day19_sub(state.resources, state.blueprint[1]),
		           aoc2022_day19_mul(state.robots, state.wait_time[1])
		       ),
		       aoc2022_day19_add(state.robots, [1, 0, 0, 0])
		FROM state
		WHERE NOT state.build_geode_now AND state.build_ore

		UNION ALL

		SELECT state.minutes - state.wait_time[2],
		       state.id,
		       state.blueprint,
		       aoc2022_day19_add(
		           aoc2022_day19_sub(state.resources, state.blueprint[2]),
		           aoc2022_day19_mul(state.robots, state.wait_time[2])
		       ),
		       aoc2022_day19_add(state.robots, [0, 1, 0, 0])
		FROM state
		WHERE NOT state.build_geode_now AND state.build_clay

		UNION ALL

		SELECT state.minutes - state.wait_time[3],
		       state.id,
		       state.blueprint,
		       aoc2022_day19_add(
		           aoc2022_day19_sub(state.resources, state.blueprint[3]),
		           aoc2022_day19_mul(state.robots, state.wait_time[3])
		       ),
		       aoc2022_day19_add(state.robots, [0, 0, 1, 0])
		FROM state
		WHERE NOT state.build_geode_now AND state.build_obsidian

		UNION ALL

		SELECT state.minutes - state.wait_time[4],
		       state.id,
		       state.blueprint,
		       aoc2022_day19_add(
		           aoc2022_day19_sub(state.resources, state.blueprint[4]),
		           aoc2022_day19_mul(state.robots, state.wait_time[4])
		       ),
		       aoc2022_day19_add(state.robots, [0, 0, 0, 1])
		FROM state
		WHERE NOT state.build_geode_now AND state.build_geode

		UNION ALL

		SELECT state.minutes - 1,
		       state.id,
		       state.blueprint,
		       aoc2022_day19_add(state.resources, state.robots),
		       state.robots
		FROM state
		WHERE NOT (
			state.build_geode_now OR state.build_ore OR state.build_clay
			OR state.build_obsidian OR state.build_geode
		)
	)
),
geodes(id, geodes) AS (
	SELECT id, max(resources[4])
	FROM mining
	GROUP BY id
)
SELECT sum(id * geodes) AS quality_level,
       sum(geodes) AS geodes,
       count(*) AS blueprints
FROM geodes;
