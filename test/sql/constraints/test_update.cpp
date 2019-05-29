#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test constraints with updates", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// NOT NULL constraint
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER NOT NULL, j INTEGER NOT NULL)"));
	// insert a value that passes
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 2)"));
	// update to a passing value should work
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET j=3"));
	// now update the value so it doesn't pass
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=NULL"));
	REQUIRE_FAIL(con.Query("UPDATE integers SET j=NULL"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// CHECK constraint
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER CHECK(i + j < 5))"));
	// insert a value that passes
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 2)"));
	// update to a passing value should work
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET j=3"));
	// now update the value so it doesn't pass
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=10"));
	REQUIRE_FAIL(con.Query("UPDATE integers SET j=10"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

}
