CREATE MACRO aoc2022_day17_bits(value) AS
	list_sum(
		list_transform(
			range(length(value)),
			lambda bit: (reverse(value)[bit + 1] = '#')::INTEGER * 1 << bit
		)
	)::INTEGER;

CREATE MACRO aoc2022_day17_push_left(rock, chamber) AS
	CASE
	    WHEN list_bit_or(
	        list_transform(generate_series(1, length(rock)), lambda y: rock[y] << 1 & chamber[y])
	    )
	        THEN rock
	    ELSE list_transform(rock, lambda row: row << 1)
	END;

CREATE MACRO aoc2022_day17_push_right(rock, chamber) AS
	CASE
	    WHEN list_bit_or(
	        list_transform(generate_series(1, length(rock)), lambda y: rock[y] >> 1 & chamber[y])
	    )
	        THEN rock
	    ELSE list_transform(rock, lambda row: row >> 1)
	END;

CREATE MACRO aoc2022_day17_collide(rock, chamber) AS
	list_bit_or(
		list_transform(generate_series(1, length(rock)), lambda y: rock[y] & chamber[y])
	);

CREATE MACRO aoc2022_day17_merge(rock, chamber) AS
	list_transform(generate_series(1, length(rock)), lambda y: rock[y] | chamber[y]);

CREATE TABLE aoc2022_day17_rocks(id INTEGER, bits INTEGER[]);

INSERT INTO aoc2022_day17_rocks VALUES
	(0, [aoc2022_day17_bits('...####..')]),
	(1, [
		aoc2022_day17_bits('....#....'),
		aoc2022_day17_bits('...###...'),
		aoc2022_day17_bits('....#....')
	]),
	(2, [
		aoc2022_day17_bits('.....#...'),
		aoc2022_day17_bits('.....#...'),
		aoc2022_day17_bits('...###...')
	]),
	(3, [
		aoc2022_day17_bits('...#.....'),
		aoc2022_day17_bits('...#.....'),
		aoc2022_day17_bits('...#.....'),
		aoc2022_day17_bits('...#.....')
	]),
	(4, [aoc2022_day17_bits('...##....'), aoc2022_day17_bits('...##....')]);

CREATE TABLE aoc2022_day17_jets(id INTEGER, jet VARCHAR);

INSERT INTO aoc2022_day17_jets
SELECT id::INTEGER, substr(pattern, id % length(pattern) + 1, 1)
FROM (SELECT '<<>><<>>>><<><>>><<<>>><<>>' AS pattern), range(257) jets(id);

CREATE MACRO aoc2022_day17_rock_count() AS 800;
