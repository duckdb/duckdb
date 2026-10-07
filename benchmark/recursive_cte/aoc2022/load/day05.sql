CREATE TABLE aoc2022_day05_initial(pos INTEGER, stack VARCHAR);

INSERT INTO aoc2022_day05_initial
SELECT pos,
       string_agg(chr(ascii('A') + ((pos * 7 + depth * 11) % 26)::INTEGER), '' ORDER BY depth)
FROM range(1, 17) positions(pos), range(256) depths(depth)
GROUP BY pos;

CREATE TABLE aoc2022_day05_moves(move INTEGER, crates INTEGER, source INTEGER, target INTEGER);

INSERT INTO aoc2022_day05_moves
SELECT move::INTEGER,
       (1 + move % 3)::INTEGER,
       (1 + (move - 1) % 16)::INTEGER,
       (1 + move % 16)::INTEGER
FROM range(1, 1025) moves(move);
