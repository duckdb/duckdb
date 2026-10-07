CREATE TABLE aoc2022_day25_values(value HUGEINT);

INSERT INTO aoc2022_day25_values
SELECT (i * i * i * 97 + i * 13 + 5)::HUGEINT
FROM range(1, 4097) values(i);
