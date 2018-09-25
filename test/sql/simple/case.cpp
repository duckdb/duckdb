
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test case statement", "[case]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)");

	result =
	    con.Query("SELECT CASE WHEN test.a=11 THEN b ELSE NULL END FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {22, Value(), Value()}));
}
