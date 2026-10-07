CREATE TABLE aoc2024_day16_field(x SMALLINT, y SMALLINT, value VARCHAR);

INSERT INTO aoc2024_day16_field
SELECT x::SMALLINT, y::SMALLINT, substr(row, x::INTEGER, 1)
FROM (
	VALUES
		(1, '###############'),
		(2, '#.......#....E#'),
		(3, '#.#.###.#.###.#'),
		(4, '#.....#.#...#.#'),
		(5, '#.###.#####.#.#'),
		(6, '#.#.#.......#.#'),
		(7, '#.#.#####.###.#'),
		(8, '#...........#.#'),
		(9, '###.#.#####.#.#'),
		(10, '#...#.....#.#.#'),
		(11, '#.#.#.###.#.#.#'),
		(12, '#.....#...#.#.#'),
		(13, '#.###.#.#.#.#.#'),
		(14, '#S..#.....#...#'),
		(15, '###############')
) rows(y, row),
LATERAL generate_series(1, length(row)) columns(x);
