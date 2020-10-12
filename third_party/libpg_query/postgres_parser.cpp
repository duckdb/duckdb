#include "postgres_parser.hpp"

#include "pg_functions.hpp"
#include "parser/parser.hpp"

using namespace std;

namespace duckdb {

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

vector<duckdb_libpgquery::PGSimplifiedToken> PostgresParser::Tokenize(std::string query) {
	duckdb_libpgquery::pg_parser_init();
	return duckdb_libpgquery::tokenize(query.c_str());
}

PostgresParser::~PostgresParser()  {
    duckdb_libpgquery::pg_parser_cleanup();
}

}
