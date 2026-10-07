CREATE TABLE aoc2022_day20_input(pos BIGINT, num BIGINT);

INSERT INTO aoc2022_day20_input
SELECT i AS pos,
       CASE WHEN i = 0 THEN 0 WHEN i % 2 = 0 THEN i ELSE -i END AS num
FROM range(256) values(i);

CREATE MACRO aoc2022_day20_modulo(a, b) AS ((a) % (b) + (b)) % (b);
CREATE MACRO aoc2022_day20_move(num, pos, size) AS
	-1 + aoc2022_day20_modulo(
		pos + aoc2022_day20_modulo(num, size - 1) + 1.5,
		size
	);
