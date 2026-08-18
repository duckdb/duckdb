WITH RECURSIVE
sudoku(puzzle, board, blank) AS (
	SELECT puzzle, board, list_position(board, 0) - 1
	FROM asql_sudoku_puzzles
	UNION ALL
	SELECT candidates.puzzle,
	       candidates.board,
	       list_position(candidates.board, 0) - 1
	FROM (
		SELECT sudoku.puzzle,
		       sudoku.board[1:sudoku.blank]
		       || [fill]
		       || sudoku.board[sudoku.blank + 2:81] AS board
		FROM sudoku, generate_series(1, 9) fills(fill)
		WHERE sudoku.blank IS NOT NULL
		  AND NOT EXISTS (
			  SELECT NULL
			  FROM generate_series(1, 9) offsets(candidate_offset)
			  WHERE fill IN (
				  sudoku.board[(sudoku.blank // 9) * 9 + candidate_offset],
				  sudoku.board[sudoku.blank % 9 + (candidate_offset - 1) * 9 + 1],
				  sudoku.board[
					  ((sudoku.blank // 3) % 3) * 3
					  + (sudoku.blank // 27) * 27
					  + candidate_offset
					  + ((candidate_offset - 1) // 3) * 6
				  ]
			  )
		  )
	) AS candidates
),
solutions AS (
	SELECT puzzle, board
	FROM sudoku
	WHERE blank IS NULL
)
SELECT count(*)::BIGINT AS solutions,
       count(DISTINCT puzzle)::BIGINT AS puzzles,
       md5(
	       string_agg(
		       list_aggregate(board, 'string_agg', ''),
		       ',' ORDER BY puzzle, board
	       )
       ) AS checksum
FROM solutions;
