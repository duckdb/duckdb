WITH RECURSIVE
monkeys(monkey_count, monkey, items, op, arg, divisor, true_target, false_target, crt) AS (
	SELECT count(*) OVER (),
	       row_number() OVER () - 1,
	       input.items::BIGINT[],
	       input.op,
	       input.arg,
	       input.divisor,
	       input.true_target,
	       input.false_target,
	       product(input.divisor) OVER ()
	FROM aoc2022_day11_input input
),
middle(turn, monkey_count, monkey, items, op, arg, divisor, true_target, false_target, crt) AS (
	SELECT 0, monkeys.*
	FROM monkeys

	UNION ALL

	SELECT CASE
	           WHEN aoc2022_day11_throws(state) AND state.items = [] OR thrown.target IS NULL
	               THEN state.turn + 1
	           ELSE state.turn
	       END,
	       state.monkey_count,
	       state.monkey,
	       CASE
	           WHEN aoc2022_day11_throws(state) THEN array_pop_front(state.items)
	           WHEN state.monkey = thrown.target THEN array_push_back(state.items, thrown.item)
	           ELSE state.items
	       END,
	       state.op,
	       state.arg,
	       state.divisor,
	       state.true_target,
	       state.false_target,
	       state.crt
	FROM middle state
	LEFT JOIN (
		SELECT CASE
		           WHEN state.op = '+' THEN state.items[1] + coalesce(state.arg, state.items[1])
		           WHEN state.op = '*' THEN state.items[1] * coalesce(state.arg, state.items[1])
		       END % state.crt AS item,
		       CASE
		           WHEN item % state.divisor = 0 THEN state.true_target
		           ELSE state.false_target
		       END AS target
		FROM middle state
		WHERE aoc2022_day11_throws(state) AND state.items <> []
	) thrown ON true
	WHERE aoc2022_day11_round(state) < aoc2022_day11_rounds()
),
inspected(monkey, times) AS (
	SELECT state.monkey,
	       count(*) FILTER (WHERE state.items <> []) AS times
	FROM middle state
	WHERE aoc2022_day11_throws(state)
	  AND aoc2022_day11_round(state) < aoc2022_day11_rounds()
	GROUP BY state.monkey
	ORDER BY times DESC
	LIMIT 2
)
SELECT product(times)::BIGINT AS monkey_business,
       sum(times)::BIGINT AS top_two_inspections
FROM inspected;
