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

	// updating a primary key to an existing value fails
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=1 WHERE i=4"));
	// but updating to a non-existing value works
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=5 WHERE i=4"));

	// if we first delete a value, we can insert that value again
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=1"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1)"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i >= 4"));

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

	// inserting a conflicting value directly fails
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (3)"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// we can insert a non-conflicting value, but then updating it fails
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (4)"));
	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=3 WHERE i=4"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// we can insert a non-conflicting value, and then update it to another non-conflicting value
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (4)"));
	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=5 WHERE i=4"));
	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 5}));
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 5}));
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

	// we can create an index with pending deletes
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

	// now we commit
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con2.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Test index with versioned data from deletes", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	// local delete
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	// "1" exists for both transactions
	result = con.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=1"));

	// "1" only exists for con2
	result = con.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con2.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// rollback
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=1"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	result = con.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// local update of primary key column
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	// 1 => 4
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=4 WHERE i=1"));

	result = con.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con2.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT i FROM integers WHERE i=4");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	result = con2.Query("SELECT i FROM integers WHERE i=4");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	// delete 4
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=4"));

	result = con.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con2.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT i FROM integers WHERE i=4");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con2.Query("SELECT i FROM integers WHERE i=4");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	// commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con2.Query("SELECT i FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT i FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	result = con2.Query("SELECT i FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
}

TEST_CASE("Test index with versioned data from updates in secondary columns", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 1), (2, 2), (3, 3)"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET j=4 WHERE i=1"));

	result = con.Query("SELECT j FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	result = con2.Query("SELECT j FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	result = con.Query("SELECT j FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT j FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test abort of update/delete", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 1), (2, 2), (3, 3)"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	// insert the value "4" for both transactions
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (4, 4)"));
	REQUIRE_NO_FAIL(con2.Query("INSERT INTO integers VALUES (4, 4)"));

	// perform some other operations
	REQUIRE_NO_FAIL(con2.Query("UPDATE integers SET j=j+1"));
	REQUIRE_NO_FAIL(con2.Query("DELETE FROM integers WHERE i=2"));
	REQUIRE_NO_FAIL(con2.Query("CREATE TABLE test(i INTEGER)"));

	// commit both transactions, con2 should now fail
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_FAIL(con2.Query("COMMIT"));

	// verify that the data is (1, 1), (...), (4, 4)
	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4}));

	// table test should not exist
	REQUIRE_FAIL(con.Query("SELECT * FROM test"));
}
