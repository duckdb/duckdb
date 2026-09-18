CREATE TYPE aoc2022_day19_vec4 AS INTEGER[4];

CREATE MACRO aoc2022_day19_infinity() AS 1000000;
CREATE MACRO aoc2022_day19_add(x, y) AS
	[x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4]]::aoc2022_day19_vec4;
CREATE MACRO aoc2022_day19_sub(x, y) AS
	[x[1] - y[1], x[2] - y[2], x[3] - y[3], x[4] - y[4]]::aoc2022_day19_vec4;
CREATE MACRO aoc2022_day19_mul(x, n) AS
	[x[1] * n, x[2] * n, x[3] * n, x[4] * n]::aoc2022_day19_vec4;
CREATE MACRO aoc2022_day19_max_minerals(blueprint, mineral) AS
	if(mineral = 4, aoc2022_day19_infinity(),
	   greatest(blueprint[1][mineral], blueprint[2][mineral],
	            blueprint[3][mineral], blueprint[4][mineral]));
CREATE MACRO aoc2022_day19_wait(minerals, robots) AS
	1 + coalesce(greatest(
		if(minerals[1] > 0 AND robots[1] > 0, ceil(minerals[1] / robots[1]), if(minerals[1] > 0, aoc2022_day19_infinity(), NULL)),
		if(minerals[2] > 0 AND robots[2] > 0, ceil(minerals[2] / robots[2]), if(minerals[2] > 0, aoc2022_day19_infinity(), NULL)),
		if(minerals[3] > 0 AND robots[3] > 0, ceil(minerals[3] / robots[3]), if(minerals[3] > 0, aoc2022_day19_infinity(), NULL)),
		if(minerals[4] > 0 AND robots[4] > 0, ceil(minerals[4] / robots[4]), if(minerals[4] > 0, aoc2022_day19_infinity(), NULL))
	)::INTEGER, 0);

CREATE TABLE aoc2022_day19_input(
	id INTEGER,
	blueprint aoc2022_day19_vec4[]
);

INSERT INTO aoc2022_day19_input VALUES
	(1, [[4, 0, 0, 0], [4, 0, 0, 0], [4, 12, 0, 0], [4, 0, 19, 0]]),
	(2, [[4, 0, 0, 0], [4, 0, 0, 0], [2, 11, 0, 0], [2, 0, 7, 0]]),
	(3, [[3, 0, 0, 0], [3, 0, 0, 0], [2, 13, 0, 0], [3, 0, 12, 0]]),
	(4, [[2, 0, 0, 0], [3, 0, 0, 0], [3, 18, 0, 0], [2, 0, 19, 0]]);
