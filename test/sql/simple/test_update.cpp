#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test string UPDATE", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 'hello'), (12, "
	                          "'world'), (13, 'blablabla')"));

	REQUIRE_NO_FAIL(con.Query("UPDATE test SET b='hello';"));
}

TEST_CASE("Test UPDATE with NULL value", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3)"));

	// update a to NULL
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=NULL;"));
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	// rollback
	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// do the same but this time commit
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=NULL;"));
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
}
