#include "postgres_parser.hpp"

extern "C" {
#include "pg_functions.h"
#include "parser/parser.h"
}

using namespace postgres;
using namespace std;

PostgresParser::PostgresParser() : success(false), parse_tree(nullptr), error_message(""), error_location(0) {};


void PostgresParser::Parse(string query) {
	pg_parser_init();
	parse_result res;
	pg_parser_parse(query.c_str(), &res);
	success = res.success;

	if (success) {
		parse_tree = res.parse_tree;
	} else {
		error_message = string(res.error_message);
		error_location = res.error_location;
	}
};

PostgresParser::~PostgresParser()  {
	pg_parser_cleanup();
};
