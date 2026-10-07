WITH RECURSIVE
hops(source, destination, distance, visited) AS (
	SELECT DISTINCT ON (valve)
	       valve,
	       valve,
	       0,
	       [valve]::VARCHAR[]
	FROM aoc2022_day16_input

	UNION ALL

	SELECT hops.destination,
	       input.tunnel,
	       hops.distance + 1,
	       array_append(hops.visited, input.tunnel)
	FROM hops, aoc2022_day16_input input
	WHERE hops.destination = input.valve
	  AND NOT array_contains(hops.visited, input.tunnel)
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
