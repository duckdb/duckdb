#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parser_options.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Extension parser adds semicolon", "[parser-extension]") {
	duckdb::DuckDB db;
	duckdb::Connection conn(*db.instance);
	duckdb::ParserExtension parser_extension;

	struct TestParserInfo : public duckdb::ParserExtensionInfo {
		std::string received_query;
	};

	auto info = duckdb::make_shared_ptr<TestParserInfo>();

	parser_extension.parser_info = info;
	parser_extension.parse_function = [](duckdb::ParserExtensionInfo *info,
	                                     const std::string &query) -> duckdb::ParserExtensionParseResult {
		auto *test_info = static_cast<TestParserInfo *>(info);
		test_info->received_query = query;
		duckdb::unique_ptr<duckdb::ParserExtensionParseData> empty_data {};
		return duckdb::ParserExtensionParseResult {std::move(empty_data)};
	};

	duckdb::ParserOptions options;
	duckdb::vector<duckdb::ParserExtension> parser_extensions {parser_extension};
	options.extensions = &parser_extensions;

	auto query = "CREATE DATABASE FROM";

	duckdb::Parser parser {options};
	REQUIRE_NOTHROW(parser.ParseQuery(query));
	REQUIRE(info->received_query == query);

	auto query_with_semicolon = "CREATE DATABASE FROM;";

	REQUIRE_NOTHROW(parser.ParseQuery(query_with_semicolon));
	REQUIRE(info->received_query == query_with_semicolon);
}

TEST_CASE("Parser Extension query splitting", "[parser-extension]") {
	DuckDB db;
	Connection conn(*db.instance);
	ParserExtension parser_extension;
	parser_extension.parse_function = [](ParserExtensionInfo *, const std::string &) -> ParserExtensionParseResult {
		duckdb::unique_ptr<ParserExtensionParseData> empty_data {};
		return ParserExtensionParseResult {std::move(empty_data)};
	};

	ParserOptions options;
	duckdb::vector<ParserExtension> parser_extensions {parser_extension};
	options.extensions = &parser_extensions;

	auto query = GENERATE( // Normal CTAS query
	    "create or syntaxerror table my_object as from (\n	select 1\n	where starts_with('name', "
	    "'test_simple_share') -- ;	\n);",
	    // Extension query, replacing the table with result
	    "create or replace result my_object as from (\n	select 1\n	where starts_with('name', "
	    "'test_simple_share') -- ;	\n);",
	    // Trying other queries
	    "CREATE OR replace result my_object as FROM (SELECT 1); -- ;");

	Parser parser {options};
	REQUIRE_NOTHROW(parser.ParseQuery(query));
	REQUIRE(parser.statements.size() == 1);
}

TEST_CASE("Parser Extension multi-query splitting", "[parser-extension]") {
	DuckDB db;
	Connection conn(*db.instance);
	ParserExtension parser_extension;
	parser_extension.parse_function = [](ParserExtensionInfo *, const std::string &) -> ParserExtensionParseResult {
		duckdb::unique_ptr<ParserExtensionParseData> empty_data {};
		return ParserExtensionParseResult {std::move(empty_data)};
	};

	ParserOptions options;
	duckdb::vector<ParserExtension> parser_extensions {parser_extension};
	options.extensions = &parser_extensions;

	auto multi_statement_query = GENERATE( // Normal CTAS query
	    "CREATE OR REPLACE result my_object as FROM (SELECT 1); -- ; \n SELECT 1;");

	Parser parser {options};
	REQUIRE_NOTHROW(parser.ParseQuery(multi_statement_query));
	REQUIRE(parser.statements.size() == 2);
}
