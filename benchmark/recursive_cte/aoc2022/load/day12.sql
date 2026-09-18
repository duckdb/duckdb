CREATE MACRO aoc2022_day12_height(height) AS
	CASE
		WHEN height = 'S' THEN ascii('a')
		WHEN height = 'E' THEN ascii('z')
		ELSE ascii(height)
	END;
