#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Single PRIMARY KEY constraint", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));

	// insert two conflicting pairs at the same time
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (3, 4), (3, 5)"));

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

	// update NULL is also not allowed
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=NULL;"));

	// insert the same value from multiple connections
	// NOTE: this tests current behavior
	// this can potentially change in the future
	Connection con2(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	// insert from first connection succeeds
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (7, 8);"));
	// insert from second connection also succeeds
	REQUIRE_NO_FAIL(con2.Query("INSERT INTO integers VALUES (7, 33);"));

	// now committing the first transaction works
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	// but the second transaction results in a conflict
	REQUIRE_FAIL(con2.Query("COMMIT"));
}

TEST_CASE("PRIMARY KEY prefix stress test multiple columns", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	//! create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, PRIMARY KEY(a, b));"));

	//! Insert 300 values
	for (idx_t idx = 0; idx < 300; idx++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(idx) + ", 'hello_" + to_string(idx) + "')"));
	}

	//! Inserting same values should fail
	for (idx_t idx = 0; idx < 300; idx++) {
		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(idx) + ", 'hello_" + to_string(idx) + "')"));
	}

	//! Update integer a on 1000 should work since there are no duplicates
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=a+1000;"));

	//! Now inserting same 1000 values should work
	for (idx_t idx = 0; idx < 300; idx++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(idx) + ", 'hello_" + to_string(idx) + "')"));
	}

	//! This update should fail and stress test the deletes on hello_ prefixes
	REQUIRE_FAIL(con.Query("UPDATE test SET a=a+1000;"));

	//! Should fail for same reason as above, just checking element per element to see if no one is escaping
	for (idx_t idx = 0; idx < 300; idx++) {
		REQUIRE_FAIL(
		    con.Query("INSERT INTO test VALUES (" + to_string(idx + 1000) + ", 'hello_" + to_string(idx) + "')"));
	}
}

TEST_CASE("Test appending the same value many times to a primary key column", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)");
	// insert a bunch of values into the index and query the index
	for (int32_t val = 0; val < 100; val++) {
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {0}));

		con.Query("INSERT INTO integers VALUES ($1)", val);

		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	for (int32_t val = 0; val < 100; val++) {
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i + i = " + to_string(val) + "+" + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	// now insert the same values, this should fail this time
	for (int32_t it = 0; it < 10; it++) {
		int32_t val = 64;
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i + i = 64+" + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		result = con.Query("INSERT INTO integers VALUES ($1)", val);
		REQUIRE_FAIL(result);
	}

	// now test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {100}));
	REQUIRE(CHECK_COLUMN(result, 1, {100}));
}

TEST_CASE("PRIMARY KEY and concurency conflicts", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	// con starts a transaction and modifies the second value
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=4 WHERE i=2"));

	// con2 can't update the second value
	REQUIRE_FAIL(con2.Query("UPDATE integers SET i=4 WHERE i=2"));
	REQUIRE_FAIL(con2.Query("UPDATE integers SET i=5 WHERE i=2"));
	// nor can it delete it
	REQUIRE_FAIL(con2.Query("DELETE FROM integers WHERE i=2"));

	// we tried to set i=5 in con2 but it failed, we can set it in con1 now though
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=5 WHERE i=3"));
	// rollback con1
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// now we can perform the changes in con2
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("UPDATE integers SET i=4 WHERE i=2"));
	REQUIRE_NO_FAIL(con2.Query("UPDATE integers SET i=5 WHERE i=3"));

	// check the results, con1 still gets the old results
	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	result = con2.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4, 5}));

	// now commit
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	// check the results again, both get the same (new) results now
	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4, 5}));
	result = con2.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4, 5}));
}
