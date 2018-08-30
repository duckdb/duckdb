
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test UNION/EXCEPT/INTERSECT", "[union]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	result = con.Query("SELECT 1 UNION SELECT 2");
	CHECK_COLUMN(result, 0, {1, 2});

	result = con.Query("SELECT 1, 'a' UNION SELECT 2, 'b'");
	CHECK_COLUMN(result, 0, {1, 2});
	CHECK_COLUMN(result, 1, {"a", "b"});

	// create tables
	result = con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	result = con.Query("INSERT INTO test VALUES (11, 1)");
	result = con.Query("INSERT INTO test VALUES (12, 2)");
	result = con.Query("INSERT INTO test VALUES (13, 3)");

	// simple cross product + join condition
	result = con.Query("SELECT a FROM test WHERE a < 13 UNION ALL SELECT a "
	                   "FROM test WHERE a = 13");

	CHECK_COLUMN(result, 0, {11, 12, 13});
}
