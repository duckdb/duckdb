
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test HAVING clause", "[aggregations]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)");

	// simple HAVING clause
	result = con.Query("SELECT b, SUM(a) AS sum FROM test GROUP BY b HAVING "
	                   "sum < 20 ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));

	// HAVING without alias
	result = con.Query("SELECT b, SUM(a) AS sum FROM test GROUP BY b HAVING "
	                   "SUM(a) < 20 ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	REQUIRE(result->column_count() == 2);

	// HAVING on column not in aggregate
	result = con.Query("SELECT b, SUM(a) AS sum FROM test GROUP BY b HAVING "
	                   "COUNT(*) = 1 ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	REQUIRE(result->column_count() == 2);
}
