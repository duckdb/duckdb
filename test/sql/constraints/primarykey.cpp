
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Single PRIMARY KEY constraint", "[constraints]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));

	// insert unique values
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 4), (2, 5)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5}));

	// insert a duplicate value as part of a chain of values
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (6, 6), (3, 4);"));

	// now insert just the first value
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (6, 6);"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5, 6}));

	// insert NULL value in PRIMARY KEY is not allowed
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (NULL, 4);"));

	// insert the same value from multiple connections
	DuckDBConnection con2(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	// insert from first connection succeeds
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (7, 8);"));
	// insert from second connection fails because of potential conflict
	// (this test is a bit strange, because it tests current behavior more than
	//  correct behavior; in postgres for example this would hang forever
	//  while waiting for the other transaction to finish)
	REQUIRE_FAIL(con2.Query("INSERT INTO integers VALUES (7, 33);"));

	REQUIRE_NO_FAIL(con.Query("COMMIT"));
}

TEST_CASE("Multiple PRIMARY KEY constraint", "[constraints]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE integers(i INTEGER, j VARCHAR, PRIMARY KEY(i, j))"));

	// insert unique values
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO integers VALUES (3, 'hello'), (3, 'world')"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "world"}));

	// insert a duplicate value as part of a chain of values
	REQUIRE_FAIL(
	    con.Query("INSERT INTO integers VALUES (6, 'bla'), (3, 'hello');"));

	// now insert just the first value
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (6, 'bla');"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 3, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "world", "bla"}));
}

// TEST_CASE("PRIMARY KEY and transactions", "[constraints]") {
// 	unique_ptr<DuckDBResult> result;
// 	DuckDB db(nullptr);
// 	DuckDBConnection con(db);

// 	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

// 	// rollback
// 	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
// 	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1);"));
// 	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

// 	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1);"));

// 	result = con.Query("SELECT * FROM integers");
// 	REQUIRE(CHECK_COLUMN(result, 0, {1}));
// }
