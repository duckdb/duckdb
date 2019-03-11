#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Create table from SELECT statement", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3), (4), (5)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 5}));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers SELECT i+3 FROM integers;"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 5, 6, 7, 8}));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers2 AS SELECT i, i+2 AS j FROM integers;"));

	result = con.Query("SELECT * FROM integers2 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 5, 6, 7, 8}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 6, 7, 8, 9, 10}));

	result = con.Query("SELECT i FROM integers2 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 5, 6, 7, 8}));

	result = con.Query("SELECT j FROM integers2 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 6, 7, 8, 9, 10}));
}
