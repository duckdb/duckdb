#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test NULL versions", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (NULL, 3)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// begin a transaction in con2
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	// update the row in con1
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=1,j=1"));

	// con1 should see the value "1"
	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));

	// con2 should see the value "NULL"
	result = con2.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));

	// after a rollback con2 should see the value "1" as well
	REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
}
