#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar queries", "[scalarquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("SELECT 42 + 1");
	REQUIRE(CHECK_COLUMN(result, 0, {43}));

	result = con.Query("SELECT 2 * (42 + 1), 35 - 2");
	REQUIRE(CHECK_COLUMN(result, 0, {86}));
	REQUIRE(CHECK_COLUMN(result, 1, {33}));

	result = con.Query("SELECT 'hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));

	result = con.Query("SELECT cast('3' AS INTEGER)");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	result = con.Query("SELECT cast(3 AS VARCHAR)");
	REQUIRE(CHECK_COLUMN(result, 0, {"3"}));

	result = con.Query("SELECT CASE WHEN 43 > 33 THEN 43 ELSE 33 END;");
	REQUIRE(CHECK_COLUMN(result, 0, {43}));

	// cannot reference columns like this
	REQUIRE_FAIL(con.Query("SELECT 1 AS a, a * 2"));
	// query without selection list
	REQUIRE_FAIL(con.Query("SELECT"));
	REQUIRE_FAIL(con.Query("SELECT FROM (SELECT 42) v1"));
}

TEST_CASE("Test scalar queries from SQLLogicTests", "[scalarquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT + CASE WHEN NOT + 84 NOT BETWEEN - 78 + 98 * 51 AND - ( ( - 28 ) ) * COUNT ( * ) + + - "
	                   "65 THEN NULL ELSE 16 / + 34 + + - 98 END / + 70 - ( - - CASE - COALESCE ( + 73, + - 66 * - 89 "
	                   "* - 72 ) WHEN COUNT ( * ) / + 4 * CAST ( - - 18 AS INTEGER ) + + + COUNT ( * ) - - 88 THEN "
	                   "NULL WHEN 92 THEN NULL ELSE COUNT ( * ) END ) AS col0");
	REQUIRE(CHECK_COLUMN(result, 0, {-2}));
}
