#include "catch.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/peg/sql_formatter.hpp"

using namespace duckdb;

TEST_CASE("SQL formatter preserves parameter spelling and literal contents", "[sql_formatter_parameters]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (auto keyword_case : {KeywordCase::UPPER, KeywordCase::LOWER, KeywordCase::PRESERVE}) {
		FormatterConfig config;
		config.keyword_case = keyword_case;
		for (auto parameter : {"$customer", "$MiXeD", "$select", "$year", "$1"}) {
			auto formatted = FormatSQL(string("SELECT ") + parameter + " AS value", config);
			REQUIRE(formatted.find(parameter) != string::npos);
			REQUIRE(formatted.find("$ ") == string::npos);
			auto prepared = con.Prepare(formatted);
			REQUIRE(!prepared->HasError());
			identifier_map_t<BoundParameterData> arguments;
			arguments.emplace(Identifier(string(parameter).substr(1)), BoundParameterData(Value::INTEGER(42)));
			auto result = prepared->Execute(arguments);
			REQUIRE_NO_FAIL(*result);
			REQUIRE(CHECK_COLUMN(result, 0, {42}));
		}
	}

	auto positional = con.Prepare(FormatSQL("SELECT '$customer, $select' AS literal, $1::INTEGER AS value"));
	REQUIRE(!positional->HasError());
	auto positional_result = positional->Execute(42);
	REQUIRE_NO_FAIL(*positional_result);
	REQUIRE(CHECK_COLUMN(positional_result, 0, {"$customer, $select"}));
	REQUIRE(CHECK_COLUMN(positional_result, 1, {42}));
}

TEST_CASE("SQL formatter preserves keyword parameters when expanding CASE expressions", "[sql_formatter_parameters]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (auto threshold : {idx_t(20), FormatterConfig().inline_threshold}) {
		FormatterConfig config;
		config.inline_threshold = threshold;
		for (auto parameter : {"$when", "$else", "$end", "$case"}) {
			const auto sql = string("SELECT CASE WHEN ") + parameter +
			                 " THEN repeat('x', 120) ELSE repeat('y', 120) END AS "
			                 "very_long_result_column_alias_0123456789";
			auto formatted = FormatSQL(sql, config);
			INFO(formatted);
			REQUIRE(formatted.find(parameter) != string::npos);
			REQUIRE(formatted.find("SELECT CASE\n") == 0);
			auto prepared = con.Prepare(formatted);
			REQUIRE(!prepared->HasError());
			for (auto condition : {true, false}) {
				identifier_map_t<BoundParameterData> arguments;
				arguments.emplace(Identifier(string(parameter).substr(1)),
				                  BoundParameterData(Value::BOOLEAN(condition)));
				auto result = prepared->Execute(arguments);
				REQUIRE_NO_FAIL(*result);
				REQUIRE(CHECK_COLUMN(result, 0, {string(120, condition ? 'x' : 'y')}));
			}
		}
	}
}
