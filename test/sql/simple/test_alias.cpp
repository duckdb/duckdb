#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test that aliases work properly in renaming columns", "[alias]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	result = con.Query("SELECT i % 2 AS p, SUM(i) AS sum_i FROM integers GROUP BY p ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 4}));
	REQUIRE(result->names[0] == "p");
	REQUIRE(result->names[1] == "sum_i");

	result = con.Query("SELECT i + 1 + 1 + 1 AS k, abs(i) AS l FROM integers WHERE i=1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(result->names[0] == "k");
	REQUIRE(result->names[1] == "l");

	result = con.Query("SELECT i AS k, i IN (1) AS l, i >= 10 AS m, 1=0 AS n FROM integers WHERE i=1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {true}));
	REQUIRE(CHECK_COLUMN(result, 2, {false}));
	REQUIRE(CHECK_COLUMN(result, 3, {false}));
	REQUIRE(result->names[0] == "k");
	REQUIRE(result->names[1] == "l");
	REQUIRE(result->names[2] == "m");
	REQUIRE(result->names[3] == "n");

	result =
	    con.Query("SELECT CASE WHEN i=1 THEN 19 ELSE 0 END AS k, i::VARCHAR AS l FROM integers WHERE i=1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {19}));
	REQUIRE(CHECK_COLUMN(result, 1, {"1"}));
	REQUIRE(result->names[0] == "k");
	REQUIRE(result->names[1] == "l");
}
