CREATE TABLE asql_fluid_config(width INTEGER, iterations INTEGER, rate DOUBLE);
INSERT INTO asql_fluid_config VALUES (256, 300, 0.125);

CREATE TABLE asql_fluid_initial(x INTEGER PRIMARY KEY, value DOUBLE);
INSERT INTO asql_fluid_initial
SELECT x::INTEGER,
       CASE
	       WHEN x BETWEEN 32 AND 63 THEN 8.0
	       WHEN x BETWEEN 160 AND 207 THEN 5.0
	       ELSE 0.0
       END
FROM asql_fluid_config, range(width) cells(x);
