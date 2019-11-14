#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test constraints with updates", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// CHECK constraint
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER CHECK(i + j < 5), k INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 2, 4)"));

	// updating values that are not referenced in the CHECK should just work
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET k=7"));
	// update to a passing value should work
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=i, j=3"));
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=i, j=3"));
	// now update the value so it doesn't pass
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=i, i=10"));
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=i, j=10"));
	// now update the value without explicitly mentioning the other column
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET j=2"));
	REQUIRE_FAIL(con.Query("UPDATE integers SET j=10"));
	// verify that the final result is correct
	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	REQUIRE(CHECK_COLUMN(result, 2, {7}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));

	// NOT NULL constraint
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER NOT NULL, j INTEGER NOT NULL)"));
	// insert a value that passes
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 2)"));
	// update to a passing value should work
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET j=3"));
	// now update the value so it doesn't pass
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=NULL"));
	REQUIRE_FAIL(con.Query("UPDATE integers SET j=NULL"));
	// verify that the final result is correct
	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
}
