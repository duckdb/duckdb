
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test ALTER TABLE RENAME COLUMN", "[alter][.]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// CREATE TABLE AND ALTER IT TO RENAME A COLUMN
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(
	    con.Query("ALTER TABLE test RENAME COLUMN i TO k"));

	result = con.Query(
	    "SELECT * FROM test");
	REQUIRE(result->column_count() == 2);
	REQUIRE(result->names[0] == "k");
	REQUIRE(result->names[1] == "j");

	// DROP TABLE IF EXISTS
	REQUIRE_NO_FAIL(con.Query("DROP TABLE IF EXISTS test"));
}

TEST_CASE("Test ALTER TABLE RENAME COLUMN with transactions", "[alter]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	DuckDBConnection con2(db);

	// CREATE TABLE
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));


	// start two transactions
	REQUIRE_NO_FAIL(con.Query("START TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("START TRANSACTION"));

	// rename column in first transaction
	REQUIRE_NO_FAIL(
	    con.Query("ALTER TABLE test RENAME COLUMN i TO k"));

	// first transaction should see the new name
	REQUIRE_FAIL(con.Query("SELECT i FROM test"));
	REQUIRE_NO_FAIL(con.Query("SELECT k FROM test"));

	// second transaction should still consider old name
	REQUIRE_NO_FAIL(con2.Query("SELECT i FROM test"));
	REQUIRE_FAIL(con2.Query("SELECT k FROM test"));

	// now commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	// second transaction should still see old name
	REQUIRE_NO_FAIL(con2.Query("SELECT i FROM test"));
	REQUIRE_FAIL(con2.Query("SELECT k FROM test"));

	// now rollback the second transasction
	// it should now see the new name
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	REQUIRE_FAIL(con.Query("SELECT i FROM test"));
	REQUIRE_NO_FAIL(con.Query("SELECT k FROM test"));
}

TEST_CASE("Test ALTER TABLE RENAME COLUMN with rollback", "[alter]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// CREATE TABLE
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));


	// rename the column
	REQUIRE_NO_FAIL(con.Query("START TRANSACTION"));

	// rename column in first transaction
	REQUIRE_NO_FAIL(
	    con.Query("ALTER TABLE test RENAME COLUMN i TO k"));

	// now we should see the new name
	REQUIRE_FAIL(con.Query("SELECT i FROM test"));
	REQUIRE_NO_FAIL(con.Query("SELECT k FROM test"));

	// rollback
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// now we should see the old name again
	REQUIRE_NO_FAIL(con.Query("SELECT i FROM test"));
	REQUIRE_FAIL(con.Query("SELECT k FROM test"));
}


TEST_CASE("Test failure conditions of ALTER TABLE", "[alter]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// CREATE TABLE AND ALTER IT TO RENAME A COLUMN
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));

	// rename a column that does not exist
	REQUIRE_FAIL(
	    con.Query("ALTER TABLE test RENAME COLUMN blablabla TO k"));

	// rename a column to an already existing column
	REQUIRE_FAIL(
	    con.Query("ALTER TABLE test RENAME COLUMN i TO j"));

	// after failure original columns should still be there
	REQUIRE_NO_FAIL(
	    con.Query("SELECT i, j FROM test"));
}

