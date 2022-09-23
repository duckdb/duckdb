#include "postgres_parser.hpp"

#include "pg_functions.hpp"
#include "parser/parser.hpp"
#include "parser/scansup.hpp"
#include "common/keywords.hpp"

using namespace std;

namespace duckdb {

PostgresParser::PostgresParser() : success(false), parse_tree(nullptr), error_message(""), error_location(0) {}

void PostgresParser::Parse(const string &query) {
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

vector<duckdb_libpgquery::PGSimplifiedToken> PostgresParser::Tokenize(const std::string &query) {
	duckdb_libpgquery::pg_parser_init();
	auto tokens = duckdb_libpgquery::tokenize(query.c_str());
	duckdb_libpgquery::pg_parser_cleanup();
	return tokens;
}

PostgresParser::~PostgresParser()  {
    duckdb_libpgquery::pg_parser_cleanup();
}

bool PostgresParser::IsKeyword(const std::string &text) {
	return duckdb_libpgquery::is_keyword(text.c_str());
}

vector<duckdb_libpgquery::PGKeyword> PostgresParser::KeywordList() {
	return duckdb_libpgquery::keyword_list();
}

void PostgresParser::SetPreserveIdentifierCase(bool preserve) {
	duckdb_libpgquery::set_preserve_identifier_case(preserve);
}

}
