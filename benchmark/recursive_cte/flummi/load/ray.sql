CREATE TABLE triangles (
	id INTEGER PRIMARY KEY,
	material VARCHAR,
	r REAL,
	g REAL,
	b REAL,
	p1_x REAL,
	p1_y REAL,
	p1_z REAL,
	p2_x REAL,
	p2_y REAL,
	p2_z REAL,
	p3_x REAL,
	p3_y REAL,
	p3_z REAL
);

CREATE TABLE spheres (
	id INTEGER PRIMARY KEY,
	material VARCHAR,
	r REAL,
	g REAL,
	b REAL,
	center_x REAL,
	center_y REAL,
	center_z REAL,
	radius REAL
);

INSERT INTO triangles VALUES
	(0, 'm', 0.6, 0.6, 0.6, -10.0, -1.0, -10.0, 10.0, -1.0, -10.0, 0.0, -1.0, 5.0),
	(1, 'm', 0.6, 0.6, 0.6, -10.0, 1.0, -10.0, 10.0, 1.0, -10.0, 0.0, 1.0, 5.0),
	(2, 'm', 0.85, 0.5, 0.0, -1.0, -10.0, -10.0, -1.0, 10.0, -10.0, -1.0, 0.0, 5.0),
	(3, 'm', 0.2, 0.6, 0.75, 1.0, -10.0, -10.0, 1.0, 10.0, -10.0, 1.0, 0.0, 5.0),
	(4, 'm', 0.6, 0.6, 0.6, -10.0, -10.0, 1.0, 10.0, -10.0, 1.0, 0.0, 5.0, 1.0),
	(5, 'm', 0.6, 0.6, 0.6, -100.0, -100.0, -10.0, 100.0, -100.0, -10.0, 0.0, 50.0, -10.0);

INSERT INTO spheres VALUES
	(0, 'l', NULL, NULL, NULL, 0.0, 0.95, 0.0, 0.35),
	(1, 'r', NULL, NULL, NULL, -0.5, -0.6, 0.3, 0.40),
	(2, 'r', NULL, NULL, NULL, 0.4, -0.6, -0.6, 0.40),
	(3, 'm', 0.2, 0.8, 0.2, 1.0, 0.0, 0.5, 0.25);
