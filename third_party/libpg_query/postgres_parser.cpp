#include "postgres_parser.hpp"

#include "pg_functions.hpp"
#include "parser/parser.hpp"

using namespace duckdb;
using namespace std;

PostgresParser::PostgresParser() : success(false), parse_tree(nullptr), error_message(""), error_location(0) {}


void PostgresParser::Parse(string query) {
	duckdb_libpgquery::pg_parser_init();
    duckdb_libpgquery::parse_result res;
	pg_parser_parse(query.c_str(), &res);
	success = res.success;

	if (success) {
		parse_tree = res.parse_tree;
	} else {
		error_message = string(res.error_message);
		error_location = res.error_location;
	}
}

string PostgresParser::FormatErrorMessage(string query, string error_message, int error_loc) {
	error_loc--;
	if (error_loc < 0 || size_t(error_loc) >= query.size()) {
		// no location in query provided
		return error_message;
	}
	size_t error_location = size_t(error_loc);
	// count the line numbers until the error location
	size_t line_number = 1;
	for(size_t i = 0; i < error_location; i++) {
		if (query[i] == '\n' || query[i] == '\r') {
			line_number++;
		}
	}
	// scan the query backwards from the error location to find the first newline
	// we also show max 40 characters before the error location, to deal with long lines
	size_t start_pos = 0;
	for(size_t i = error_location; i > 0 && error_location - i < 40; i--) {
		if (query[i - 1] == '\n' || query[i - 1] == '\r') {
			start_pos = i;
			break;
		}
	}
	// find the end of the line, or max 40 characters AFTER the error location
	size_t end_pos = query.size() - 1;
	for(size_t i = error_location; i < query.size() - 1 && i - error_location < 40; i++) {
		if (query[i] == '\n' || query[i] == '\r') {
			end_pos = i;
			break;
		}
	}
	string line_indicator = "LINE " + to_string(line_number) + ": ";

	// now first print the error message plus the current line (or a subset of the line)
	error_message += "\n" + line_indicator + query.substr(start_pos, end_pos - start_pos);
	// print an arrow pointing at the error location
	error_message += "\n" + string(error_location - start_pos + line_indicator.size(), ' ') + "^";
	return error_message;
}

PostgresParser::~PostgresParser()  {
    duckdb_libpgquery::pg_parser_cleanup();
}
