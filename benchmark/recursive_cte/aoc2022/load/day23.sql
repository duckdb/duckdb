CREATE MACRO aoc2022_day23_v2(x, y) AS {x: x, y: y};
CREATE MACRO aoc2022_day23_add(v1, v2) AS
	aoc2022_day23_v2(v1.x + v2.x, v1.y + v2.y);
CREATE MACRO aoc2022_day23_byte(bits) AS
	list_sum(list_transform(range(8), lambda i: bits[8 - i] * 1 << i));
CREATE MACRO aoc2022_day23_directions() AS
	[
		{bits: aoc2022_day23_byte([1, 0, 0, 0, 1, 0, 0, 1]), dir: aoc2022_day23_v2(0, -1)},
		{bits: aoc2022_day23_byte([0, 1, 0, 0, 0, 1, 1, 0]), dir: aoc2022_day23_v2(0, 1)},
		{bits: aoc2022_day23_byte([0, 0, 1, 0, 1, 0, 1, 0]), dir: aoc2022_day23_v2(-1, 0)},
		{bits: aoc2022_day23_byte([0, 0, 0, 1, 0, 1, 0, 1]), dir: aoc2022_day23_v2(1, 0)}
	];
CREATE MACRO aoc2022_day23_direction(delta_x, delta_y) AS
	list_extract([
		aoc2022_day23_byte([0, 0, 0, 0, 1, 0, 0, 0]),
		aoc2022_day23_byte([0, 0, 1, 0, 0, 0, 0, 0]),
		aoc2022_day23_byte([0, 0, 0, 0, 0, 0, 1, 0]),
		aoc2022_day23_byte([1, 0, 0, 0, 0, 0, 0, 0]),
		aoc2022_day23_byte([0, 0, 0, 0, 0, 0, 0, 0]),
		aoc2022_day23_byte([0, 1, 0, 0, 0, 0, 0, 0]),
		aoc2022_day23_byte([0, 0, 0, 0, 0, 0, 0, 1]),
		aoc2022_day23_byte([0, 0, 0, 1, 0, 0, 0, 0]),
		aoc2022_day23_byte([0, 0, 0, 0, 0, 1, 0, 0])
	], (delta_x + 1) * 3 + (delta_y + 1) + 1);
CREATE MACRO aoc2022_day23_propose(vicinity, directions) AS
	coalesce(
		directions[list_indexof(
			list_transform(directions, lambda direction: vicinity & direction.bits),
			0
		)].dir,
		aoc2022_day23_v2(0, 0)
	);
CREATE MACRO aoc2022_day23_rotate(values) AS values[2:] || [values[1]];

CREATE TABLE aoc2022_day23_scan AS
SELECT row_number() OVER (ORDER BY y, x) AS id,
       aoc2022_day23_v2(x::INTEGER, y::INTEGER) AS xy
FROM range(18) xs(x), range(18) ys(y)
WHERE (x * 17 + y * 31 + x * y) % 5 < 2;
