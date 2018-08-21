
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test DISTINCT keyword", "[distinct]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 22)");
	con.Query("INSERT INTO test VALUES (13, 22)");
	con.Query("INSERT INTO test VALUES (11, 21)");
	con.Query("INSERT INTO test VALUES (11, 22)");

	result = con.Query("SELECT DISTINCT * FROM test ORDER BY a, b");
	CHECK_COLUMN(result, 0, {11, 11, 13});
	CHECK_COLUMN(result, 1, {21, 22, 22});

	result = con.Query("SELECT DISTINCT a FROM test ORDER BY a");
	CHECK_COLUMN(result, 0, {11, 13});

	result = con.Query("SELECT DISTINCT b FROM test ORDER BY b");
	CHECK_COLUMN(result, 0, {21, 22});

	result =
	    con.Query("SELECT DISTINCT a, SUM(B) FROM test GROUP BY a ORDER BY a");
	CHECK_COLUMN(result, 0, {11, 13});
	CHECK_COLUMN(result, 1, {65, 22});

	result = con.Query("SELECT DISTINCT MAX(b) FROM test GROUP BY a");
	CHECK_COLUMN(result, 0, {22});

	result = con.Query(
	    "SELECT DISTINCT CASE WHEN a > 11 THEN 11 ELSE a END FROM test");
	CHECK_COLUMN(result, 0, {11});
}
