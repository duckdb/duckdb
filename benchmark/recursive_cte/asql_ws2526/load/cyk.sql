CREATE TABLE asql_cyk_config(token_count INTEGER, max_iterations INTEGER);
INSERT INTO asql_cyk_config VALUES (255, 67);

CREATE TABLE asql_cyk_grammar(
	lhs VARCHAR,
	terminal VARCHAR,
	left_symbol VARCHAR,
	right_symbol VARCHAR,
	start_symbol BOOLEAN
);

INSERT INTO asql_cyk_grammar VALUES
	('expression', NULL, 'expression', 'sum', true),
	('expression', NULL, 'term', 'product', true),
	('expression', 'n', NULL, NULL, true),
	('term', NULL, 'term', 'product', false),
	('term', 'n', NULL, NULL, false),
	('sum', NULL, 'plus', 'term', false),
	('product', NULL, 'multiply', 'number', false),
	('number', 'n', NULL, NULL, false),
	('plus', '+', NULL, NULL, false),
	('multiply', '*', NULL, NULL, false);

CREATE TABLE asql_cyk_tokens(position INTEGER, token VARCHAR);
INSERT INTO asql_cyk_tokens
SELECT position::INTEGER + 1,
       CASE
	       WHEN position % 2 = 0 THEN 'n'
	       WHEN (position // 2) % 2 = 0 THEN '+'
	       ELSE '*'
       END
FROM asql_cyk_config, range(token_count) positions(position);
