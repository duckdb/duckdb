
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test UNION/EXCEPT/INTERSECT", "[union]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	result = con.Query("SELECT 1 UNION ALL SELECT 2");
	CHECK_COLUMN(result, 0, {1, 2});

	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 2, 'b'");
	CHECK_COLUMN(result, 0, {1, 2});
	CHECK_COLUMN(result, 1, {"a", "b"});

	result = con.Query(
	    "SELECT 1, 'a' UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'");
	CHECK_COLUMN(result, 0, {1, 2, 3});
	CHECK_COLUMN(result, 1, {"a", "b", "c"});

	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 2, 'b' UNION ALL SELECT "
	                   "3, 'c' UNION ALL SELECT 4, 'd'");
	CHECK_COLUMN(result, 0, {1, 2, 3, 4});
	CHECK_COLUMN(result, 1, {"a", "b", "c", "d"});

	// create tables
	result = con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	result = con.Query("INSERT INTO test VALUES (11, 1), (12, 1), (13, 2)");

	// UNION ALL, no unique results
	result = con.Query("SELECT a FROM test WHERE a < 13 UNION ALL SELECT a "
	                   "FROM test WHERE a = 13");
	CHECK_COLUMN(result, 0, {11, 12, 13});

	result = con.Query("SELECT b FROM test WHERE a < 13 UNION ALL SELECT b "
	                   "FROM test WHERE a > 11");
	CHECK_COLUMN(result, 0, {1, 1, 1, 2});

	// mixing types, should upcast
	result = con.Query("SELECT 1 UNION ALL SELECT 'asdf'");
	CHECK_COLUMN(result, 0, {"1", "asdf"});

	result = con.Query("SELECT NULL UNION ALL SELECT 'asdf'");
	CHECK_COLUMN(result, 0, {Value(), "asdf"});

	// only UNION, distinct results

	result = con.Query("SELECT 1 UNION SELECT 1");
	CHECK_COLUMN(result, 0, {1});

	result = con.Query("SELECT 1, 'a' UNION SELECT 2, 'b' UNION SELECT 3, 'c' "
	                   "UNION SELECT 1, 'a'");
	CHECK_COLUMN(result, 0, {1, 2, 3});
	CHECK_COLUMN(result, 1, {"a", "b", "c"});

	result = con.Query("SELECT b FROM test WHERE a < 13 UNION  SELECT b FROM "
	                   "test WHERE a > 11");
	CHECK_COLUMN(result, 0, {1, 2});

	// mixed fun
	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 1, 'a' UNION SELECT 2, "
	                   "'b' UNION SELECT 1, 'a'");
	CHECK_COLUMN(result, 0, {1, 1, 2});
	CHECK_COLUMN(result, 1, {"a", "a", "b"});
}
