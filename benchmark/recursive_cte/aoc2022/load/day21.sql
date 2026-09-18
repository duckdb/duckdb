CREATE TABLE aoc2022_day21_monkeys(
	id INTEGER,
	left_child INTEGER,
	right_child INTEGER,
	value BIGINT
);

INSERT INTO aoc2022_day21_monkeys
SELECT id::INTEGER,
       CASE WHEN id < 4095 THEN (2 * id + 1)::INTEGER ELSE NULL END,
       CASE WHEN id < 4095 THEN (2 * id + 2)::INTEGER ELSE NULL END,
       CASE WHEN id >= 4095 THEN (id * 13 + 7)::BIGINT ELSE NULL END
FROM range(8191) monkeys(id);
