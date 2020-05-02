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

	SECTION("CHECK constraint") {
		// create a table with a check constraint referencing the to-be-renamed column
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER CHECK(i < 10), j INTEGER)"));
		// insert some elements
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (i, j) VALUES (1, 2), (2, 3)"));
		REQUIRE_FAIL(con.Query("INSERT INTO test (i, j) VALUES (100, 2)"));
		// now alter the column name
		// currently, we don't support altering tables with constraints
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test RENAME COLUMN i TO k"));
		// the check should still work after the alter table
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (k, j) VALUES (1, 2), (2, 3)"));
		REQUIRE_FAIL(con.Query("INSERT INTO test (k, j) VALUES (100, 2)"));
	}
	SECTION("NOT NULL constraint") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER NOT NULL, j INTEGER)"));
		// insert some elements
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (i, j) VALUES (1, 2), (2, 3)"));
		REQUIRE_FAIL(con.Query("INSERT INTO test (i, j) VALUES (NULL, 2)"));
		// now alter the column name
		// currently, we don't support altering tables with constraints
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test RENAME COLUMN i TO k"));
		// the check should still work after the alter table
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (k, j) VALUES (1, 2), (2, 3)"));
		REQUIRE_FAIL(con.Query("INSERT INTO test (k, j) VALUES (NULL, 2)"));
	}
	SECTION("UNIQUE constraint") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER, PRIMARY KEY(i, j))"));
		// insert some elements
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (i, j) VALUES (1, 1), (2, 2)"));
		REQUIRE_FAIL(con.Query("INSERT INTO test (i, j) VALUES (1, 1)"));
		// now alter the column name
		// currently, we don't support altering tables with constraints
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test RENAME COLUMN i TO k"));
		// the check should still work after the alter table
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (k, j) VALUES (3, 3), (4, 4)"));
		REQUIRE_FAIL(con.Query("INSERT INTO test (k, j) VALUES (1, 1)"));
	}
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

TEST_CASE("Test ALTER TABLE DROP COLUMN", "[alter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 1), (2, 2)"));

	SECTION("Standard DROP COLUMN") {
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test DROP COLUMN j"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(result->names.size() == 1);
	}
	SECTION("Rollback of DROP COLUMN") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test DROP COLUMN j"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(result->names.size() == 1);
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
		REQUIRE(result->names.size() == 2);
	}
	SECTION("Cannot DROP COLUMN which has an index built on it") {
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON test(j)"));
		REQUIRE_FAIL(con.Query("ALTER TABLE test DROP COLUMN j"));

		// we can remove the column after dropping the index
		REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test DROP COLUMN j"));
	}
	SECTION("DROP COLUMN with check constraint on single column") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(i INTEGER, j INTEGER CHECK(j < 10))"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 1), (2, 2)"));
		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));

		// we can drop a column that has a single check constraint on it
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test2 DROP COLUMN j"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (3)"));

		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(result->names.size() == 1);
	}
	SECTION("DROP COLUMN with check constraint on multiple columns") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(i INTEGER, j INTEGER CHECK(i+j < 10))"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 1), (2, 2)"));
		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));

		// we CANNOT drop one of the columns, because the CHECK constraint depends on both
		REQUIRE_FAIL(con.Query("ALTER TABLE test2 DROP COLUMN j"));
	}
	SECTION("DROP COLUMN with NOT NULL constraint") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(i INTEGER, j INTEGER NOT NULL)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 1), (2, 2)"));
		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));

		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test2 DROP COLUMN j"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (3)"));

		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(result->names.size() == 1);
	}
	SECTION("DROP COLUMN with check constraint on subsequent column") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(i INTEGER, j INTEGER CHECK(j < 10))"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 1), (2, 2)"));
		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));

		// we can drop a column that has a single check constraint on it
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test2 DROP COLUMN i"));
		REQUIRE_FAIL(con.Query("INSERT INTO test2 VALUES (20)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (3)"));

		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(result->names.size() == 1);
	}
	SECTION("DROP COLUMN with NOT NULL constraint on subsequent column") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(i INTEGER, j INTEGER, k INTEGER NOT NULL)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 1, 11), (2, 2, 12)"));
		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {11, 12}));

		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test2 DROP COLUMN j"));
		REQUIRE_FAIL(con.Query("INSERT INTO test2 VALUES (3, NULL)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (3, 13)"));

		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 1, {11, 12, 13}));
		REQUIRE(result->names.size() == 2);
	}
	SECTION("DROP COLUMN with index built on subsequent column") {
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON test(j)"));

		// cannot drop indexed column
		REQUIRE_FAIL(con.Query("ALTER TABLE test DROP COLUMN j"));
		// we also cannot drop the column i (for now) because an index depends on a subsequent column
		REQUIRE_FAIL(con.Query("ALTER TABLE test DROP COLUMN i"));
	}
	SECTION("DROP COLUMN from table with primary key constraint") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(i INTEGER PRIMARY KEY, j INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 1), (2, 2)"));

		// cannot drop primary key column
		REQUIRE_FAIL(con.Query("ALTER TABLE test2 DROP COLUMN i"));
		// but we can drop column "i"
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test2 DROP COLUMN j"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (3)"));

		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(result->names.size() == 1);
	}
	SECTION("DROP COLUMN errors") {
		// cannot drop column which does not exist
		REQUIRE_FAIL(con.Query("ALTER TABLE test DROP COLUMN blabla"));
		// unless IF EXISTS is specified
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test DROP COLUMN IF EXISTS blabla"));

		// cannot drop ALL columns of a table
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test DROP COLUMN i"));
		REQUIRE_FAIL(con.Query("ALTER TABLE test DROP COLUMN j"));
	}
}

TEST_CASE("Test ALTER TABLE DROP COLUMN with multiple transactions", "[alter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 1), (2, 2)"));

	SECTION("Only one pending table alter can be active at a time") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		// con removes a column to test
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test DROP COLUMN j"));
		// con2 cannot add a new column now!
		REQUIRE_FAIL(con2.Query("ALTER TABLE test ADD COLUMN k INTEGER"));
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
		// we can add the column after the commit
		REQUIRE_NO_FAIL(con2.Query("ALTER TABLE test ADD COLUMN k INTEGER"));
	}
	SECTION("Can only append to newest table") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		// con removes a column from test
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test DROP COLUMN i"));

		// con2 cannot append now!
		REQUIRE_FAIL(con2.Query("INSERT INTO test (i, j) VALUES (3, 3)"));
		// but we can delete rows!
		REQUIRE_NO_FAIL(con2.Query("DELETE FROM test WHERE i=1"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

		result = con2.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
		REQUIRE(CHECK_COLUMN(result, 1, {2}));

		// we can also update rows
		REQUIRE_NO_FAIL(con2.Query("UPDATE test SET j=100"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

		result = con2.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
		REQUIRE(CHECK_COLUMN(result, 1, {100}));

		REQUIRE_NO_FAIL(con.Query("COMMIT"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {100}));
	}
	SECTION("Alter table while other transaction still has pending appends") {
		REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO test VALUES (3, 3)"));

		// now con adds a column
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test DROP COLUMN i"));

		// cannot commit con2! conflict on append
		REQUIRE_FAIL(con2.Query("COMMIT"));
	}
	SECTION("Create index on column that has been removed by other transaction") {
		// con2 removes a column
		REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con2.Query("ALTER TABLE test DROP COLUMN j"));

		// now con tries to add an index to that column: this should fail
		REQUIRE_FAIL(con.Query("CREATE INDEX i_index ON test(j"));
	}
}

TEST_CASE("Test ALTER TABLE SET DEFAULT", "[alter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 1), (2, 2)"));

	REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER j SET DEFAULT 3"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (i) VALUES (3)"));
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));

	REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER COLUMN j DROP DEFAULT"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (i) VALUES (4)"));
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, Value()}));

	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER j SET DEFAULT nextval('seq')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test (i) VALUES (5), (6)"));
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, Value(), 1, 2}));

	// fail when column does not exist
	REQUIRE_FAIL(con.Query("ALTER TABLE test ALTER blabla SET DEFAULT 3"));
	REQUIRE_FAIL(con.Query("ALTER TABLE test ALTER blabla DROP DEFAULT"));
}

TEST_CASE("Test ALTER TABLE ALTER TYPE", "[alter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 1), (2, 2)"));

	SECTION("Standard ALTER TYPE") {
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER i SET DATA TYPE VARCHAR"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {"1", "2"}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	}
	SECTION("ALTER TYPE with expression") {
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER i TYPE BIGINT USING i+100"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {101, 102}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	}
	SECTION("Rollback ALTER TYPE") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER i SET DATA TYPE VARCHAR"));
		REQUIRE_NO_FAIL(con.Query("UPDATE test SET i='hello'"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {"hello", "hello"}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	}
	SECTION("ALTER TYPE with transaction local data") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3, 3)"));
		// not currently supported
		REQUIRE_FAIL(con.Query("ALTER TABLE test ALTER i SET DATA TYPE BIGINT"));
	}
	SECTION("ALTER TYPE with expression using multiple columns") {
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER i TYPE INTEGER USING 2*(i+j)"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {4, 8}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	}
	SECTION("ALTER TYPE with NOT NULL constraint") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(i INTEGER NOT NULL, j INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 1), (2, 2)"));
		REQUIRE_FAIL(con.Query("INSERT INTO test2 VALUES (NULL, 4)"));

		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test2 ALTER i SET DATA TYPE VARCHAR"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES ('hello', 3)"));
		REQUIRE_FAIL(con.Query("INSERT INTO test2 VALUES (NULL, 4)"));

		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {"1", "2", "hello"}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	}
	SECTION("ALTER TYPE with CHECK constraint") {
		// we disallow ALTER TYPE on a column with a CHECK constraint
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(i INTEGER CHECK(i < 10), j INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 1), (2, 2)"));

		REQUIRE_FAIL(con.Query("ALTER TABLE test2 ALTER i SET DATA TYPE VARCHAR"));
	}
	SECTION("ALTER TYPE with UNIQUE constraint") {
		// we disallow ALTER TYPE on a column with a UNIQUE constraint
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(i INTEGER UNIQUE, j INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 1), (2, 2)"));

		REQUIRE_FAIL(con.Query("ALTER TABLE test2 ALTER i SET DATA TYPE VARCHAR"));
		// but we CAN change the other column
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test2 ALTER j SET DATA TYPE VARCHAR"));
		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {"1", "2"}));
	}
	SECTION("ALTER TYPE with INDEX") {
		// we disallow ALTER TYPE on a column with an index on it
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON test(i)"));
		REQUIRE_FAIL(con.Query("ALTER TABLE test ALTER i SET DATA TYPE VARCHAR"));

		// we can alter the table after the index is dropped, however
		REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER i SET DATA TYPE VARCHAR"));
	}
	SECTION("ALTER TYPE with unknown columns") {
		REQUIRE_FAIL(con.Query("ALTER TABLE test ALTER blabla SET TYPE VARCHAR"));
		REQUIRE_FAIL(con.Query("ALTER TABLE test ALTER i SET TYPE VARCHAR USING blabla"));
		// cannot use aggregates or window functions
		REQUIRE_FAIL(con.Query("ALTER TABLE test ALTER i SET TYPE VARCHAR USING SUM(i)"));
		REQUIRE_FAIL(con.Query("ALTER TABLE test ALTER i SET TYPE VARCHAR USING row_id() OVER ()"));
	}
}

TEST_CASE("Test ALTER TABLE ALTER TYPE with multiple transactions", "[alter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 1), (2, 2)"));

	SECTION("Only one pending table alter can be active at a time") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		// con alters a column to test
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER j TYPE VARCHAR"));
		// con2 cannot alter another column now!
		REQUIRE_FAIL(con2.Query("ALTER TABLE test ALTER i TYPE VARCHAR"));
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
		// we can alter the column after the commit
		REQUIRE_NO_FAIL(con2.Query("ALTER TABLE test ALTER i TYPE VARCHAR"));
	}
	SECTION("Can only append to newest table") {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		// con removes a column from test
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER i TYPE VARCHAR"));

		// con2 cannot append now!
		REQUIRE_FAIL(con2.Query("INSERT INTO test (i, j) VALUES (3, 3)"));
		// but we can delete rows!
		REQUIRE_NO_FAIL(con2.Query("DELETE FROM test WHERE i=1"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {"1", "2"}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));

		result = con2.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
		REQUIRE(CHECK_COLUMN(result, 1, {2}));

		// we can also update rows, but updates to i will not be seen...
		// should we check this somehow?
		REQUIRE_NO_FAIL(con2.Query("UPDATE test SET i=1000"));
		REQUIRE_NO_FAIL(con2.Query("UPDATE test SET j=100"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {"1", "2"}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));

		result = con2.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {1000}));
		REQUIRE(CHECK_COLUMN(result, 1, {100}));

		REQUIRE_NO_FAIL(con.Query("COMMIT"));

		result = con.Query("SELECT * FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {"2"}));
		REQUIRE(CHECK_COLUMN(result, 1, {100}));
	}
	SECTION("Alter table while other transaction still has pending appends") {
		REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO test VALUES (3, 3)"));

		// now con adds a column
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER i TYPE VARCHAR"));

		// cannot commit con2! conflict on append
		REQUIRE_FAIL(con2.Query("COMMIT"));
	}
	SECTION("Create index on column that has been altered by other transaction") {
		// con2 removes a column
		REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con2.Query("ALTER TABLE test ALTER j TYPE VARCHAR"));

		// now con tries to add an index to that column: this should fail
		REQUIRE_FAIL(con.Query("CREATE INDEX i_index ON test(j"));
	}
}
