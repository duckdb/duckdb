#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test GROUP BY on expression", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integer(i INTEGER, j INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integer VALUES (3, 4), (3, 5), (3, 7);"));
	result = con.Query("SELECT j * 2 FROM integer GROUP BY j * 2 ORDER BY j * 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {8, 10, 14}));
}

TEST_CASE("Test GROUP BY with many groups", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER);"));
	for(index_t i = 0; i < 10000; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(i) + ", 1), (" + to_string(i) + ", 2);"));
	}
	result = con.Query("SELECT SUM(i), SUM(sums) FROM (SELECT i, SUM(j) AS sums FROM integers GROUP BY i) tbl1");
	REQUIRE(CHECK_COLUMN(result, 0, {49995000}));
	REQUIRE(CHECK_COLUMN(result, 1, {30000}));
}
