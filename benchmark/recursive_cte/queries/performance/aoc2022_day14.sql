WITH RECURSIVE
sand(grain) AS (
	SELECT {
	    rest: 0,
	    x: 500,
	    y: 0,
	    path: []::STRUCT(x INTEGER, y INTEGER)[],
	    occupied: list({x: x, y: y})
	}
	FROM aoc2022_day14_rocks

	UNION ALL

	SELECT CASE
	    WHEN NOT list_contains(grain.occupied, {x: grain.x, y: grain.y + 1})
	        THEN {
	            rest: grain.rest,
	            x: grain.x,
	            y: grain.y + 1,
	            path: list_prepend({x: grain.x, y: grain.y}, grain.path),
	            occupied: grain.occupied
	        }
	    WHEN NOT list_contains(grain.occupied, {x: grain.x - 1, y: grain.y + 1})
	        THEN {
	            rest: grain.rest,
	            x: grain.x - 1,
	            y: grain.y + 1,
	            path: list_prepend({x: grain.x, y: grain.y}, grain.path),
	            occupied: grain.occupied
	        }
	    WHEN NOT list_contains(grain.occupied, {x: grain.x + 1, y: grain.y + 1})
	        THEN {
	            rest: grain.rest,
	            x: grain.x + 1,
	            y: grain.y + 1,
	            path: list_prepend({x: grain.x, y: grain.y}, grain.path),
	            occupied: grain.occupied
	        }
	    WHEN grain.x = 500 AND grain.y = 0
	        THEN {
	            rest: grain.rest + 1,
	            x: NULL,
	            y: NULL,
	            path: grain.path,
	            occupied: list_prepend({x: grain.x, y: grain.y}, grain.occupied)
	        }
	    ELSE {
	        rest: grain.rest + 1,
	        x: grain.path[1].x,
	        y: grain.path[1].y,
	        path: grain.path[2:],
	        occupied: list_prepend({x: grain.x, y: grain.y}, grain.occupied)
	    }
	END
	FROM sand
	WHERE grain.x IS NOT NULL
)
SELECT grain.rest,
       length(grain.occupied),
       md5(
           list_aggregate(
               list_sort(list_transform(grain.occupied, lambda point: point.x || ':' || point.y)),
               'string_agg',
               ','
           )
       )
FROM sand
WHERE grain.x IS NULL;
