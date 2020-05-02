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

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 1), (2, 2)"));

	SECTION("Standard ADD COLUMN") {
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN k INTEGER"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value()}));
	}
	SECTION("ADD COLUMN with default value") {
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN l INTEGER DEFAULT 3"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {3, 3}));
	}
	SECTION("ADD COLUMN with sequence as default value") {
		REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN m INTEGER DEFAULT nextval('seq')"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {1, 2}));
	}
	SECTION("ADD COLUMN with data inside local storage") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3, 3)"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN k INTEGER"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value(), Value()}));

		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
		REQUIRE(result->names.size() == 2);

		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3, 3)"));
		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	}
	SECTION("multiple ADD COLUMN in the same transaction") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3, 3)"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN k INTEGER"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN l INTEGER"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN m INTEGER DEFAULT 3"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value(), Value()}));
		REQUIRE(CHECK_COLUMN(result, 3, {Value(), Value(), Value()}));
		REQUIRE(CHECK_COLUMN(result, 4, {3, 3, 3}));

		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
		REQUIRE(result->names.size() == 2);
	}
	SECTION("ADD COLUMN with index") {
		// what if we create an index on the new column, then rollback
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN k INTEGER DEFAULT 2"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON test(k)"));
		REQUIRE_NO_FAIL(con.Query("COMMIT"));

		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3, 3, 3)"));

		result = con.Query("SELECT * FROM test WHERE k=2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {2, 2}));

		result = con.Query("SELECT * FROM test WHERE k=3");
		REQUIRE(CHECK_COLUMN(result, 0, {3}));
		REQUIRE(CHECK_COLUMN(result, 1, {3}));
		REQUIRE(CHECK_COLUMN(result, 2, {3}));
	}
	SECTION("ADD COLUMN rollback with index") {
		// what if we create an index on the new column, then rollback
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN k INTEGER"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON test(k)"));
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3, 3)"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	}
	SECTION("Incorrect usage") {
		// cannot add a column that already exists!
		REQUIRE_FAIL(con.Query("ALTER TABLE test ADD COLUMN i INTEGER"));
	}
}

TEST_CASE("Test ALTER TABLE ADD COLUMN with multiple transactions", "[alter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 1), (2, 2)"));

	SECTION("Only one pending table alter can be active at a time") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		// con adds a column to test
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN k INTEGER"));
		// con2 cannot add a new column now!
		REQUIRE_FAIL(con2.Query("ALTER TABLE test ADD COLUMN l INTEGER"));
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
		// after a commit, con2 can add a new column again
		REQUIRE_NO_FAIL(con2.Query("ALTER TABLE test ADD COLUMN l INTEGER"));
	}
	SECTION("Can only append to newest table") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		// con adds a column to test
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN k INTEGER"));

		// con2 cannot append now!
		REQUIRE_FAIL(con2.Query("INSERT INTO test (i, j) VALUES (3, 3)"));
		// but we can delete rows!
		REQUIRE_NO_FAIL(con2.Query("DELETE FROM test WHERE i=1"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value()}));

		result = con2.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
		REQUIRE(CHECK_COLUMN(result, 1, {2}));

		// we can also update rows
		REQUIRE_NO_FAIL(con2.Query("UPDATE test SET j=100"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value()}));

		result = con2.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
		REQUIRE(CHECK_COLUMN(result, 1, {100}));

		REQUIRE_NO_FAIL(con.Query("COMMIT"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
		REQUIRE(CHECK_COLUMN(result, 1, {100}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	}
	SECTION("Alter table while other transaction still has pending appends") {
		REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO test VALUES (3, 3)"));

		// now con adds a column
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN k INTEGER"));

		// cannot commit con2! conflict on append
		REQUIRE_FAIL(con2.Query("COMMIT"));
	}
}
