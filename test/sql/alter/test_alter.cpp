#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test ALTER TABLE RENAME COLUMN", "[alter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// CREATE TABLE AND ALTER IT TO RENAME A COLUMN
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE test RENAME COLUMN i TO k"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(result->names.size() == 2);
	REQUIRE(result->names[0] == "k");
	REQUIRE(result->names[1] == "j");

	// DROP TABLE IF EXISTS
	REQUIRE_NO_FAIL(con.Query("DROP TABLE IF EXISTS test"));
}

TEST_CASE("Test ALTER TABLE RENAME COLUMN and dependencies", "[alter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// prepared statements
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("PREPARE v AS SELECT * FROM test"));
	// we don't allow altering of tables when there are dependencies
	REQUIRE_FAIL(con.Query("ALTER TABLE test RENAME COLUMN i TO k"));
	// deallocate the dependency
	REQUIRE_NO_FAIL(con.Query("DEALLOCATE v"));
	// now we can alter the table
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE test RENAME COLUMN i TO k"));
	result = con.Query("SELECT * FROM test");
	REQUIRE(result->names.size() == 2);
	REQUIRE(result->names[0] == "k");
	REQUIRE(result->names[1] == "j");
	REQUIRE_NO_FAIL(con.Query("DROP TABLE IF EXISTS test"));
}

TEST_CASE("Test ALTER TABLE RENAME COLUMN with transactions", "[alter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	Connection con2(db);

	// CREATE TABLE
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));

	// start two transactions
	REQUIRE_NO_FAIL(con.Query("START TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("START TRANSACTION"));

	// rename column in first transaction
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE test RENAME COLUMN i TO k"));

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
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// CREATE TABLE
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));

	// rename the column
	REQUIRE_NO_FAIL(con.Query("START TRANSACTION"));

	// rename column in first transaction
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE test RENAME COLUMN i TO k"));

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
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// CREATE TABLE AND ALTER IT TO RENAME A COLUMN
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));

	// rename a column that does not exist
	REQUIRE_FAIL(con.Query("ALTER TABLE test RENAME COLUMN blablabla TO k"));

	// rename a column to an already existing column
	REQUIRE_FAIL(con.Query("ALTER TABLE test RENAME COLUMN i TO j"));

	// after failure original columns should still be there
	REQUIRE_NO_FAIL(con.Query("SELECT i, j FROM test"));
}

TEST_CASE("Test ALTER TABLE RENAME COLUMN on a table with constraints", "[alter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a table with a check constraint referencing the to-be-renamed column
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER CHECK(i < 10), j INTEGER)"));
	// insert some elements
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (i, j) VALUES (1, 2), (2, 3)"));
	REQUIRE_FAIL(con.Query("INSERT INTO test (i, j) VALUES (100, 2)"));
	// now alter the column name
	// currently, we don't support altering tables with constraints
	REQUIRE_FAIL(con.Query("ALTER TABLE test RENAME COLUMN i TO k"));
	// the check should still work after the alter table
	// REQUIRE_NO_FAIL(con.Query("INSERT INTO test (k, j) VALUES (1, 2), (2, 3)"));
	// REQUIRE_FAIL(con.Query("INSERT INTO test (k, j) VALUES (100, 2)"));
}

TEST_CASE("Test ALTER TABLE ADD COLUMN", "[alter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// CREATE TABLE AND ALTER IT TO RENAME A COLUMN
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 1), (2, 2)"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));

	REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN k INTEGER"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value()}));

	// add a column with a default value
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN l INTEGER DEFAULT 3"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {3, 3}));

	// default value as a sequence
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN m INTEGER DEFAULT nextval('seq')"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {3, 3}));
	REQUIRE(CHECK_COLUMN(result, 4, {1, 2}));
}
