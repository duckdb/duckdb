WITH RECURSIVE
hops(source, destination, distance) USING KEY (source, destination) AS (
	SELECT DISTINCT valve,
	       valve,
	       0
	FROM aoc2022_day16_input

	UNION ALL

	(
		WITH candidates AS (
			SELECT hops.destination AS source,
			       input.tunnel AS destination,
			       hops.distance + 1 AS distance
			FROM hops, aoc2022_day16_input input
			WHERE hops.destination = input.valve
		),
		new_destinations AS (
			SELECT candidates.source,
			       candidates.destination,
			       min(candidates.distance) AS distance
			FROM candidates
			LEFT JOIN recurring.hops visited
			  ON candidates.source = visited.source
			 AND candidates.destination = visited.destination
			WHERE visited.source IS NULL
			GROUP BY candidates.source, candidates.destination
		)
		SELECT * FROM new_destinations
	)
),
distances(source, destination, distance, flow) AS MATERIALIZED (
	SELECT hops.source,
	       hops.destination,
	       min(hops.distance) + 1,
	       destination.flow
	FROM hops
	JOIN (SELECT DISTINCT valve, flow FROM aoc2022_day16_input) source
	  ON source.valve = hops.source
	JOIN (SELECT DISTINCT valve, flow FROM aoc2022_day16_input) destination
	  ON destination.valve = hops.destination
	WHERE (source.flow <> 0 OR source.valve = 'AA')
	  AND destination.flow <> 0
	  AND hops.source <> hops.destination
	GROUP BY hops.source, hops.destination, destination.flow
),
flow(time, valve, released, visited) AS (
	SELECT 30, 'AA', 0, []::VARCHAR[]

	UNION ALL

	SELECT flow.time - distances.distance,
	       distances.destination,
	       flow.released + (flow.time - distances.distance) * distances.flow,
	       array_append(flow.visited, distances.destination)
	FROM flow, distances
	WHERE flow.valve = distances.source
	  AND NOT array_contains(flow.visited, distances.destination)
	  AND flow.time - distances.distance >= 0
)
SELECT max(released) AS released,
       count(*) AS explored_states,
       max(len(visited)) AS opened_valves
FROM flow;
