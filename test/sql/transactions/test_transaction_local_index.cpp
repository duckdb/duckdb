#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test index with transaction local commits", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// first test simple index usage
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	// inserting a duplicate value fails
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (1)"));
	// inserting a non-duplicate value works
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (4)"));

	// updating primary keys is disallowed
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=1 WHERE i=4"));
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=5 WHERE i=4"));

	// if we first delete a value, we can insert that value again
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=1"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1)"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=4"));

	result = con.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con2.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// now test with multiple transactions
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	// both transactions can insert the same value
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (4)"));
	REQUIRE_NO_FAIL(con2.Query("INSERT INTO integers VALUES (4)"));

	result = con.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	result = con2.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));

	// also using the index is fine
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=4");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT COUNT(*) FROM integers WHERE i=4");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// conflict happens on commit
	// we can commit con
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	// but then con2 fails to commit
	REQUIRE_FAIL(con2.Query("COMMIT"));
}

TEST_CASE("Test index with transaction local updates", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	// inserting a conflicting value directly fails
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (3)"));
	// we can insert a non-conflicting value
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (4)"));
	// but then updating it again fails
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=3 WHERE i=4"));
}

TEST_CASE("Test index with pending insertions", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// we can create an index with pending insertions
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con2.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// after committing, the values are added to the index
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test index with pending updates", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// we cannot create an index with pending updates
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	// update a value
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("UPDATE integers SET i=4 WHERE i=1"));

	// failed to create an index: pending updates
	REQUIRE_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

	// now we commit
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	// no more pending updates: creating the index works now
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=4");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT COUNT(*) FROM integers WHERE i=4");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test index with pending deletes", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// we can create an index with pending deletes
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	// delete a value
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("DELETE FROM integers WHERE i=1"));

	// we cannot create the index with a pending delete
	REQUIRE_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

	// now we commit
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	// now we can create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con2.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

