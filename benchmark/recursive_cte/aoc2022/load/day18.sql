CREATE TABLE aoc2022_day18_input(x INTEGER, y INTEGER, z INTEGER);

INSERT INTO aoc2022_day18_input
SELECT x::INTEGER, y::INTEGER, z::INTEGER
FROM range(24) xs(x), range(24) ys(y), range(24) zs(z)
WHERE x IN (0, 23) OR y IN (0, 23) OR z IN (0, 23);
