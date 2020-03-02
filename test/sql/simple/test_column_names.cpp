#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Check if column names are correctly set in the result", "[sql]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (42, 10), (43, 100);"));

	// check column names for simple projections and aliases
	result = con.Query("SELECT a, b, a * 2 AS c, b * (a * 2) AS d FROM test ORDER BY a");
	REQUIRE(result->names.size() == 4);
	REQUIRE(result->names[0] == "a");
	REQUIRE(result->names[1] == "b");
	REQUIRE(result->names[2] == "c");
	REQUIRE(result->names[3] == "d");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 100}));
	REQUIRE(CHECK_COLUMN(result, 2, {84, 86}));
	REQUIRE(CHECK_COLUMN(result, 3, {840, 8600}));
}
