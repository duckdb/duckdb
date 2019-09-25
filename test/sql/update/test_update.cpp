#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test standard update behavior", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3)"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// test simple update
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=1"));

	// not seen yet by con2, only by con1
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// now test a rollback
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=4"));

	// not seen yet by con2, only by con1
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	result = con2.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}
