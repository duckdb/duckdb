CREATE TABLE aoc2022_day24_config(width INTEGER, height INTEGER, repeat INTEGER);

INSERT INTO aoc2022_day24_config VALUES (17, 19, 323);

CREATE TABLE aoc2022_day24_blizzards(id INTEGER, x INTEGER, y INTEGER, dx INTEGER, dy INTEGER);

INSERT INTO aoc2022_day24_blizzards
SELECT id::INTEGER,
       ((id * 7 + 3) % 17)::INTEGER,
       ((id * 11 + 5) % 19)::INTEGER,
       CASE id % 4 WHEN 0 THEN 1 WHEN 2 THEN -1 ELSE 0 END::INTEGER,
       CASE id % 4 WHEN 1 THEN 1 WHEN 3 THEN -1 ELSE 0 END::INTEGER
FROM range(28) blizzards(id);

CREATE MACRO aoc2022_day24_modulo(a, b) AS ((a) % (b) + (b)) % (b);
