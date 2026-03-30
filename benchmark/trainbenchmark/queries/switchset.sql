SELECT
	Semaphore.id AS semaphore,
	Route.id AS route,
	SwitchPosition.id AS swP,
	Switch.id AS sw,
	SwitchPosition.position AS position,
	Switch.currentPosition AS currentPosition

-- (route)
FROM Route

-- (route)-[:follows]->(swP)
INNER JOIN SwitchPosition
ON Route.id = SwitchPosition.route -- the "SwitchPosition.route" attribute is the inverse of the "Route.follows" edge

-- (swP)-[:target]->(sw)
INNER JOIN Switch
ON SwitchPosition.target = Switch.id

-- (route)-[:entry]->(semaphore)
INNER JOIN Semaphore
ON Route.entry = Semaphore.id

WHERE Route.active = 1
	AND Switch.currentPosition != SwitchPosition.position
	AND Semaphore.signal = 2;
