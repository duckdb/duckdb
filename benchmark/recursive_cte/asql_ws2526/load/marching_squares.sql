CREATE TABLE asql_marching_config(
	width INTEGER,
	height INTEGER,
	min_x INTEGER,
	max_x INTEGER,
	min_y INTEGER,
	max_y INTEGER
);
INSERT INTO asql_marching_config VALUES (384, 384, 64, 319, 64, 319);

CREATE TABLE asql_marching_map(x INTEGER, y INTEGER, altitude INTEGER, PRIMARY KEY (x, y));
INSERT INTO asql_marching_map
SELECT x::INTEGER,
       y::INTEGER,
       CASE
	       WHEN x BETWEEN min_x AND max_x AND y BETWEEN min_y AND max_y THEN 200
	       ELSE 100
       END
FROM asql_marching_config, range(width) xs(x), range(height) ys(y);

CREATE TABLE asql_marching_directions(
	lower_left BOOLEAN,
	lower_right BOOLEAN,
	upper_left BOOLEAN,
	upper_right BOOLEAN,
	delta_x INTEGER,
	delta_y INTEGER,
	PRIMARY KEY (lower_left, lower_right, upper_left, upper_right)
);

INSERT INTO asql_marching_directions VALUES
	(false, false, false, false, 1, 0),
	(false, false, false, true, 1, 0),
	(false, false, true, false, 0, 1),
	(false, false, true, true, 1, 0),
	(false, true, false, false, 0, -1),
	(false, true, false, true, 0, -1),
	(false, true, true, false, 0, 1),
	(false, true, true, true, 0, -1),
	(true, false, false, false, -1, 0),
	(true, false, false, true, -1, 0),
	(true, false, true, false, 0, 1),
	(true, false, true, true, 1, 0),
	(true, true, false, false, -1, 0),
	(true, true, false, true, -1, 0),
	(true, true, true, false, 0, 1),
	(true, true, true, true, NULL, NULL);
