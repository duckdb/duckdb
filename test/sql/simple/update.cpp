
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test string UPDATE", "[update]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 'hello'), (12, "
	                          "'world'), (13, 'blablabla')"));

	REQUIRE_NO_FAIL(con.Query("UPDATE test SET b='hello';"));
}
