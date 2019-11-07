#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test standard update behavior with NULLs", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1), (2), (3), (NULL)"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));

	///////////////
	// test updating from a non-null value to a null value
	///////////////
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=NULL WHERE a=2"));

	// not seen yet by con2, only by con1
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 1, 3}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));

	// commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 1, 3}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 1, 3}));

	// now test a rollback
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=NULL WHERE a=3"));

	// not seen yet by con2, only by con1
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), 1}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 1, 3}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 1, 3}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 1, 3}));

	///////////////
	// test updating from a null value to a non-null value
	///////////////
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=10 WHERE a IS NULL"));

	// not seen yet by con2, only by con1
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3, 10, 10}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 1, 3}));

	// now rollback
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// values are back to original values
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 1, 3}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 1, 3}));

	// perform the same update, but this time commit
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=10 WHERE a IS NULL"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3, 10, 10}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 1, 3}));

	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3, 10, 10}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3, 10, 10}));
}
