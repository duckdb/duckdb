#include "postgres_parser.hpp"

#include "pg_functions.hpp"
#include "parser/parser.hpp"
#include "parser/scansup.hpp"
#include "common/keywords.hpp"
#include "utf8proc_wrapper.hpp"

#include <limits>

namespace duckdb {

PostgresParser::PostgresParser() : success(false), parse_tree(nullptr), error_message(""), error_location(0) {}

void PostgresParser::Parse(const std::string &query) {
	success = false;
	parse_tree = nullptr;
	error_message = "";
	error_location = 0;
	duckdb_libpgquery::pg_parser_init();

	UnicodeInvalidReason reason = UnicodeInvalidReason::INVALID_UNICODE;
	size_t invalid_pos = 0;
	auto unicode_type = Utf8Proc::Analyze(query.c_str(), query.size(), &reason, &invalid_pos);
	if (unicode_type == UnicodeType::INVALID) {
		const char *reason_str = reason == UnicodeInvalidReason::BYTE_MISMATCH ? "byte sequence mismatch"
		                                                                        : "invalid unicode";
		error_message = std::string("Invalid UTF-8 in query (") + reason_str + ")";
		if (invalid_pos >= static_cast<size_t>(std::numeric_limits<int>::max() - 1)) {
			error_location = std::numeric_limits<int>::max();
		} else {
			error_location = static_cast<int>(invalid_pos) + 1;
		}
		return;
	}

	duckdb_libpgquery::parse_result res;
	pg_parser_parse(query.c_str(), &res);
	success = res.success;

	if (success) {
		parse_tree = res.parse_tree;
	} else {
		error_message = std::string(res.error_message);
		error_location = res.error_location;
	}
}

vector<duckdb_libpgquery::PGSimplifiedToken> PostgresParser::Tokenize(const std::string &query) {
	duckdb_libpgquery::pg_parser_init();
	auto tokens = duckdb_libpgquery::tokenize(query.c_str());
	duckdb_libpgquery::pg_parser_cleanup();
	return std::move(tokens);
}

PostgresParser::~PostgresParser()  {
    duckdb_libpgquery::pg_parser_cleanup();
}

duckdb_libpgquery::PGKeywordCategory PostgresParser::IsKeyword(const std::string &text) {
	return duckdb_libpgquery::is_keyword(text.c_str());
}

vector<duckdb_libpgquery::PGKeyword> PostgresParser::KeywordList() {
	// FIXME: because of this, we might need to change the libpg_query library to use duckdb::vector
	vector<duckdb_libpgquery::PGKeyword> tmp(duckdb_libpgquery::keyword_list());
	return tmp;
}

void PostgresParser::SetPreserveIdentifierCase(bool preserve) {
	duckdb_libpgquery::set_preserve_identifier_case(preserve);
}

}
