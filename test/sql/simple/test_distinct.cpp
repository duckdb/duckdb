#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test DISTINCT keyword", "[distinct]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.EnableQueryVerification();

	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (11, 21), (11, 22)");

	result = con.Query("SELECT DISTINCT * FROM test ORDER BY a, b");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 11, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {21, 22, 22}));

	result = con.Query("SELECT DISTINCT a FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));

	result = con.Query("SELECT DISTINCT b FROM test ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));

	result = con.Query("SELECT DISTINCT a, SUM(B) FROM test GROUP BY a ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {65, 22}));

	result = con.Query("SELECT DISTINCT MAX(b) FROM test GROUP BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {22}));

	result = con.Query("SELECT DISTINCT CASE WHEN a > 11 THEN 11 ELSE a END FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));
}

TEST_CASE("Test DISTINCT and ORDER BY", "[distinct]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.EnableQueryVerification();

	con.Query("CREATE TABLE integers(i INTEGER);");
	con.Query("INSERT INTO integers VALUES (1), (2), (3)");

	result = con.Query("SELECT DISTINCT i%2 FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));

	// controversial: Postgres fails here with the error "with SELECT DISTINCT columns from ORDER BY must appear in the SELECT clause"
	// but SQLite succeeds
	// for now we fail because it gives unintuitive results
	REQUIRE_FAIL(con.Query("SELECT DISTINCT i%2 FROM integers ORDER BY i"));
}
