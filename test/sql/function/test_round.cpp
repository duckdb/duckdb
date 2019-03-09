#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Rounding test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE roundme(a DOUBLE, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO roundme VALUES (42.123456, 3)"));

	result = con.Query("select round(42.12345, 0)");
	REQUIRE(CHECK_COLUMN(result, 0, {42.0}));

	result = con.Query("select round(42.12345, 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {42.12}));

	result = con.Query("select round(42, 0)");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("select round(a, 1) from roundme");
	REQUIRE(CHECK_COLUMN(result, 0, {42.1}));

	result = con.Query("select round(b, 1) from roundme");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	result = con.Query("select round(a, b) from roundme");
	REQUIRE(CHECK_COLUMN(result, 0, {42.123}));
}
