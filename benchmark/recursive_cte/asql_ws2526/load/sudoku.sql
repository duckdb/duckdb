CREATE TABLE asql_sudoku_config(puzzle_count INTEGER);
INSERT INTO asql_sudoku_config VALUES (32);

CREATE TABLE asql_sudoku_puzzles(puzzle INTEGER PRIMARY KEY, board INTEGER[]);
INSERT INTO asql_sudoku_puzzles
SELECT puzzle::INTEGER,
       list(
	       CASE
		       WHEN (position * 17 + 5) % 81 < 24 THEN 0
		       ELSE ((position // 9) * 3 + (position // 9) // 3 + position % 9) % 9 + 1
	       END
	       ORDER BY position
       )::INTEGER[]
FROM asql_sudoku_config,
     range(puzzle_count) puzzles(puzzle),
     range(81) cells(position)
GROUP BY puzzle;
