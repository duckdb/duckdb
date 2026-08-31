CREATE MACRO aoc2022_day11_rounds() AS 1000;
CREATE MACRO aoc2022_day11_throws(m) AS m.turn % m.monkey_count = m.monkey;
CREATE MACRO aoc2022_day11_round(m) AS m.turn // m.monkey_count;

CREATE TABLE aoc2022_day11_input AS
SELECT *
FROM (VALUES
	([79, 98],         '*', 19,   23, 2, 3),
	([54, 65, 75, 74], '+', 6,    19, 2, 0),
	([79, 60, 97],     '*', NULL, 13, 1, 3),
	([74],             '+', 3,    17, 0, 1)
) input(items, op, arg, divisor, true_target, false_target);
