
#include "parser/parser.hpp"
#include "parser/pg_list.h"
#include "parser/pg_query.h"
#include "parser/pg_trigger.h"

#include <stdio.h>

Parser::Parser() {}

void Parser::ParseQuery(const char *query) {
	auto ctx = pg_query_parse_init();
	auto result = pg_query_parse(query);
	if (result.error) {
		fprintf(stderr, "%s at %d\n", result.error->message,
		        result.error->cursorpos);
	} else {
		fprintf(stderr, "Successfully parsed query.\n");
	}

	pg_query_parse_finish(ctx);
	pg_query_free_parse_result(result);
}
