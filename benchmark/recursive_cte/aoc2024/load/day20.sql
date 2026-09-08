CREATE TABLE aoc2024_day20_field(x SMALLINT, y SMALLINT, value VARCHAR);

INSERT INTO aoc2024_day20_field
SELECT x::SMALLINT,
       y::SMALLINT,
       CASE
           WHEN x = 0 OR y = 0 OR x = 25 OR y = 25 THEN '#'
           WHEN x = 1 AND y = 1 THEN 'S'
           WHEN x = 24 AND y = 24 THEN 'E'
           WHEN x % 8 = 0 AND y % 8 NOT IN (1, 2) THEN '#'
           ELSE '.'
       END
FROM range(26) xs(x), range(26) ys(y);
