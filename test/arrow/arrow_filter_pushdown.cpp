#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"

using namespace duckdb;

// Helper: create an ArrowTestFactory from a query result (all rows, no filter)
static unique_ptr<ArrowTestFactory> MakeArrowFactory(Connection &con, const string &query, bool use_string_view) {
	if (use_string_view) {
		REQUIRE(!con.Query("SET produce_arrow_string_view = true")->HasError());
		REQUIRE(!con.Query("SET arrow_output_version = '1.5'")->HasError());
	} else {
		REQUIRE(!con.Query("SET produce_arrow_string_view = false")->HasError());
		REQUIRE(!con.Query("SET arrow_output_version = '1.0'")->HasError());
	}
	auto client_properties = con.context->GetClientProperties();
	auto result = con.context->Query(query, false);
	REQUIRE(!result->HasError());
	auto types = result->types;
	auto names = result->names;
	return make_uniq<ArrowTestFactory>(std::move(types), std::move(names), std::move(result), false, client_properties,
	                                   *con.context);
}

// Helper: get the EXPLAIN output for an arrow_scan with a filter
static string GetExplainForFilter(Connection &con, ArrowTestFactory &factory, const string &filter_expr) {
	const auto params = ArrowTestHelper::ConstructArrowScan(factory);
	const auto rel = con.TableFunction("arrow_scan", params)->Filter(filter_expr);
	const auto explain_result = rel->Explain();
	REQUIRE(!explain_result->HasError());
	auto &mat = explain_result->Cast<MaterializedQueryResult>();
	return mat.GetValue(1, 0).ToString();
}

// Helper: check for a standalone FILTER operator node in the explain output
static bool StandaloneFilter(const std::string &explain_str) {
	// Look for FILTER as a standalone operator node name (not "Filters:" which is a scan attribute)
	std::string::size_type pos = 0;
	while ((pos = explain_str.find("FILTER", pos)) != std::string::npos) {
		// Check this is the standalone node name, not part of "Filters:"
		if (pos + 6 >= explain_str.size() || explain_str[pos + 6] != 's') {
			return true;
		}
		pos += 6;
	}
	return false;
}

// Helper: check for a scan node with filter pushdown in the explain output
static bool FilterInScan(const std::string &explain_str) {
	auto arrow_pos = explain_str.find("ARROW_SCAN");
	if (arrow_pos == std::string::npos) {
		return false;
	}
	return explain_str.find("Filters:", arrow_pos) != std::string::npos;
}

TEST_CASE("Arrow filter pushdown - view types disable pushdown", "[arrow]") {
	DuckDB db;
	Connection con(db);

	// Create a test table with an INT id column and a VARCHAR name column
	REQUIRE(!con.Query("CREATE TABLE src AS SELECT i AS id, i::VARCHAR AS name FROM range(10) tbl(i)")->HasError());

	SECTION("String view column: filter above scan (not pushed)") {
		auto factory = MakeArrowFactory(con, "SELECT * FROM src", true);
		auto explain_str = GetExplainForFilter(con, *factory, "id > 5");
		REQUIRE(StandaloneFilter(explain_str));
		REQUIRE(!FilterInScan(explain_str));
	}

	SECTION("Regular string column: filter pushed into scan") {
		auto factory = MakeArrowFactory(con, "SELECT * FROM src", false);
		auto explain_str = GetExplainForFilter(con, *factory, "id > 5");
		REQUIRE(!StandaloneFilter(explain_str));
		REQUIRE(FilterInScan(explain_str));
	}

	SECTION("Integer-only table: filter pushed into scan") {
		auto factory = MakeArrowFactory(con, "SELECT id FROM src", false);
		auto explain_str = GetExplainForFilter(con, *factory, "id > 5");
		REQUIRE(!StandaloneFilter(explain_str));
		REQUIRE(FilterInScan(explain_str));
	}
}

TEST_CASE("Arrow filter pushdown - nested view types disable pushdown", "[arrow]") {
	DuckDB db;
	Connection con(db);

	REQUIRE(!con.Query("CREATE TABLE src AS SELECT i AS id, 'val_' || i::VARCHAR AS name FROM range(10) tbl(i)")
	             ->HasError());

	SECTION("Struct containing string_view") {
		auto factory = MakeArrowFactory(con, "SELECT id, {'s': name} AS nested FROM src", true);
		auto explain_str = GetExplainForFilter(con, *factory, "id > 5");
		REQUIRE(StandaloneFilter(explain_str));
		REQUIRE(!FilterInScan(explain_str));
	}

	SECTION("List containing string_view") {
		auto factory = MakeArrowFactory(con, "SELECT id, [name] AS names FROM src", true);
		auto explain_str = GetExplainForFilter(con, *factory, "id > 5");
		REQUIRE(StandaloneFilter(explain_str));
		REQUIRE(!FilterInScan(explain_str));
	}
}
