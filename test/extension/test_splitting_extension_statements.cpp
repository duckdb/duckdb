#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parser_options.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Parser Extension query splitting", "[parser-extension]") {
	DuckDB db;
	Connection conn(*db.instance);
	ParserExtension parser_extension;
	parser_extension.parse_function = [](ParserExtensionInfo *,
										 const std::string &) -> ParserExtensionParseResult {
		duckdb::unique_ptr<ParserExtensionParseData> empty_data {};
		return ParserExtensionParseResult {std::move(empty_data)};
	};

	ParserOptions options;
	duckdb::vector<ParserExtension> parser_extensions {parser_extension};
	options.extensions = &parser_extensions;

	auto query = GENERATE( // Normal CTAS query
		"create or replace table my_object as from (\n	select 1\n	where starts_with('name', "
		"'test_simple_share') -- ;	\n);",
		// Extension query, replacing the table with result
		"create or replace result my_object as from (\n	select 1\n	where starts_with('name', "
		"'test_simple_share') -- ;	\n);");

	Parser parser {options};
	REQUIRE_NOTHROW(parser.ParseQuery(query));
	for (uint64_t i = 0; i < parser.statements.size(); i++) {
		auto &stmt = parser.statements[i];
		duckdb::Printer::PrintF("Parsed query %llu: %s\n", i, stmt->query.c_str());
	}
	REQUIRE(parser.statements.size() == 1);
}