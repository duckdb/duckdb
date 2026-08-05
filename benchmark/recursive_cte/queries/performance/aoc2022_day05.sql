WITH RECURSIVE
initial(stacks) AS (
	SELECT list({pos: pos, stack: stack} ORDER BY pos)
	FROM aoc2022_day05_initial
),
run(move, stacks_9000, stacks_9001) AS (
	SELECT 1, stacks, stacks
	FROM initial

	UNION ALL

	SELECT run.move + 1,
	       list_transform(run.stacks_9000, lambda stack:
	           CASE
	               WHEN stack.pos = moves.target
	                   THEN {
	                       pos: stack.pos,
	                       stack: reverse(run.stacks_9000[moves.source].stack[:moves.crates]) || stack.stack
	                   }
	               WHEN stack.pos = moves.source
	                   THEN {pos: stack.pos, stack: stack.stack[1 + moves.crates:]}
	               ELSE stack
	           END
	       ),
	       list_transform(run.stacks_9001, lambda stack:
	           CASE
	               WHEN stack.pos = moves.target
	                   THEN {
	                       pos: stack.pos,
	                       stack: run.stacks_9001[moves.source].stack[:moves.crates] || stack.stack
	                   }
	               WHEN stack.pos = moves.source
	                   THEN {pos: stack.pos, stack: stack.stack[1 + moves.crates:]}
	               ELSE stack
	           END
	       )
	FROM run
	JOIN aoc2022_day05_moves moves ON run.move = moves.move
)
SELECT move,
       md5(list_aggregate(list_transform(stacks_9000, lambda stack: stack.stack[1]), 'string_agg', '')),
       md5(list_aggregate(list_transform(stacks_9001, lambda stack: stack.stack[1]), 'string_agg', '')),
       list_sum(list_transform(stacks_9000, lambda stack: length(stack.stack)))
FROM run
WHERE move = (SELECT max(move) FROM run);
