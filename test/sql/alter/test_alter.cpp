#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

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
