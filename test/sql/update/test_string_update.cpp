#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test update of string columns", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('hello'), ('world')"));

	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION;"));

	// scan the table
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	// test a delete from the table
	REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a='hello';"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	// now test an update of the table
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='hello';"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	REQUIRE_NO_FAIL(con2.Query("COMMIT;"));

	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
}

TEST_CASE("Test update of string columns with NULLs", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('hello'), ('world')"));

	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION;"));

	// update a string to NULL
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=NULL where a='world';"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	REQUIRE_NO_FAIL(con2.Query("COMMIT;"));

	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello"}));
}

TEST_CASE("Test repeated update of string in same segment", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('hello'), ('world')"));

	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION;"));

	// scan the table
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	// test a number of repeated updates
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='test' WHERE a='hello';"));
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='test2' WHERE a='world';"));
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "test2"}));

	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	REQUIRE_NO_FAIL(con2.Query("COMMIT;"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "test2"}));
}

TEST_CASE("Test rollback of string update", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('hello'), ('world')"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));

	// perform an update within the transaction
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='test' WHERE a='hello';"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	// now rollback the update
	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));
}
