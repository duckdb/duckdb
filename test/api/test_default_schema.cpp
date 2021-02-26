#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Default Schema", "[api]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA s1;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE s1.t1(a INTEGER, b VARCHAR);"));

	// Table is not in main schema, so this should fail
	REQUIRE_FAIL(con.Query("SELECT * FROM t1;"));

	REQUIRE_NO_FAIL(con.Query("PRAGMA default_schema='s1';"));

	// Default schema is now s1, so this should succeed
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM t1;"));
}