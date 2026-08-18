WITH RECURSIVE
bounding_box(x_min, x_max, y_min, y_max, z_min, z_max) AS (
	SELECT min(x) - 1, max(x) + 1,
	       min(y) - 1, max(y) + 1,
	       min(z) - 1, max(z) + 1
	FROM aoc2022_day18_input
),
filled_box(x, y, z) AS (
	SELECT x_min, y_min, z_min
	FROM bounding_box

	UNION

	SELECT CASE WHEN filled_box.x + 1 <= box.x_max THEN filled_box.x + 1 ELSE box.x_min END,
	       CASE
	           WHEN filled_box.x + 1 <= box.x_max THEN filled_box.y
	           WHEN filled_box.y + 1 <= box.y_max THEN filled_box.y + 1
	           ELSE box.y_min
	       END,
	       CASE
	           WHEN filled_box.x + 1 <= box.x_max THEN filled_box.z
	           WHEN filled_box.y + 1 <= box.y_max THEN filled_box.z
	           WHEN filled_box.z + 1 <= box.z_max THEN filled_box.z + 1
	           ELSE box.z_max
	       END
	FROM filled_box, bounding_box box
	WHERE filled_box.z <= box.z_max
),
air(x, y, z) AS MATERIALIZED (
	SELECT * FROM filled_box
	EXCEPT
	SELECT * FROM aoc2022_day18_input
),
outside_air(x, y, z) AS (
	SELECT min(x), min(y), min(z)
	FROM air

	UNION

	SELECT candidate.x, candidate.y, candidate.z
	FROM (
		SELECT air.x, air.y, air.z
		FROM air, outside_air
		WHERE (outside_air.x = air.x - 1 AND outside_air.y = air.y AND outside_air.z = air.z)
		   OR (outside_air.x = air.x + 1 AND outside_air.y = air.y AND outside_air.z = air.z)
		   OR (outside_air.x = air.x AND outside_air.y = air.y - 1 AND outside_air.z = air.z)
		   OR (outside_air.x = air.x AND outside_air.y = air.y + 1 AND outside_air.z = air.z)
		   OR (outside_air.x = air.x AND outside_air.y = air.y AND outside_air.z = air.z - 1)
		   OR (outside_air.x = air.x AND outside_air.y = air.y AND outside_air.z = air.z + 1)
	) candidate
),
solid(x, y, z) AS MATERIALIZED (
	SELECT * FROM filled_box
	EXCEPT
	SELECT * FROM outside_air
),
adjacent_faces(count) AS (
	SELECT count(*)
	FROM solid left_cube, solid right_cube
	WHERE (abs(left_cube.x - right_cube.x) = 1 AND left_cube.y = right_cube.y AND left_cube.z = right_cube.z)
	   OR (abs(left_cube.y - right_cube.y) = 1 AND left_cube.x = right_cube.x AND left_cube.z = right_cube.z)
	   OR (abs(left_cube.z - right_cube.z) = 1 AND left_cube.x = right_cube.x AND left_cube.y = right_cube.y)
)
SELECT count(*) * 6 - adjacent_faces.count AS exterior_surface,
       count(*) AS filled_cubes
FROM solid, adjacent_faces
GROUP BY adjacent_faces.count;
