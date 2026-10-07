WITH RECURSIVE
parse(iteration, symbol, start_position, end_position) AS (
	SELECT 0,
	       grammar.lhs,
	       tokens.position,
	       tokens.position
	FROM asql_cyk_tokens AS tokens, asql_cyk_grammar AS grammar
	WHERE tokens.token = grammar.terminal
	UNION ALL
	SELECT candidates.iteration + 1,
	       candidates.symbol,
	       candidates.start_position,
	       candidates.end_position
	FROM (
		TABLE parse
		UNION
		SELECT left_parse.iteration,
		       grammar.lhs,
		       left_parse.start_position,
		       right_parse.end_position
		FROM asql_cyk_grammar AS grammar,
		     parse AS left_parse,
		     parse AS right_parse
		WHERE left_parse.end_position + 1 = right_parse.start_position
		  AND grammar.left_symbol = left_parse.symbol
		  AND grammar.right_symbol = right_parse.symbol
	) AS candidates, asql_cyk_config AS config
	WHERE candidates.iteration < config.max_iterations
),
facts AS (
	SELECT DISTINCT symbol, start_position, end_position
	FROM parse
)
SELECT count(*)::BIGINT AS facts,
       (SELECT max(iteration)::INTEGER FROM parse) AS iterations,
       EXISTS (
	       SELECT NULL
	       FROM facts, asql_cyk_grammar AS grammar, asql_cyk_config AS config
	       WHERE facts.symbol = grammar.lhs
	         AND grammar.start_symbol
	         AND facts.start_position = 1
	         AND facts.end_position = config.token_count
       )::INTEGER AS accepted,
       md5(
	       string_agg(
		       format('{}:{}:{}', facts.symbol, facts.start_position, facts.end_position),
		       ',' ORDER BY facts.symbol, facts.start_position, facts.end_position
	       )
       ) AS checksum
FROM facts;
