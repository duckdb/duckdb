CREATE TABLE aoc2022_day22_instructions(instruction INTEGER, distance INTEGER, turn INTEGER);

INSERT INTO aoc2022_day22_instructions
SELECT instruction::INTEGER,
       (5 + instruction % 23)::INTEGER,
       CASE WHEN instruction % 5 < 2 THEN 1 ELSE -1 END::INTEGER
FROM range(1, 1025) instructions(instruction);

CREATE MACRO aoc2022_day22_width() AS 97;
CREATE MACRO aoc2022_day22_height() AS 89;
