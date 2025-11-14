#include "duckdb.hpp"
#include "catch.hpp"

using namespace duckdb;

static string GetExplainPhysicalPlan(DuckDB &db, const string &query) {
	Connection con(db);
	auto result = con.Query("EXPLAIN " + query);
	REQUIRE(result->ColumnCount() == 2);
	REQUIRE(result->RowCount() >= 1);
	return result->GetValue(1, 0).ToString();
}

TEST_CASE("ColumnLifetime DISTINCT ON only scans referenced columns", "[optimizer][column_lifetime]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dcla(a INTEGER, b INTEGER, c INTEGER, unused INTEGER);"));

	auto plan = GetExplainPhysicalPlan(db, "SELECT DISTINCT ON (a) b FROM dcla ORDER BY a, c");
	REQUIRE(plan.find("Projections:") != string::npos);
	REQUIRE(plan.find(" b ") != string::npos);
	REQUIRE(plan.find(" a ") != string::npos);
	REQUIRE(plan.find(" c ") != string::npos);
	REQUIRE(plan.find("unused") == string::npos);
}
