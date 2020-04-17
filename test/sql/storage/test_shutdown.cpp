#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Shutdown with running transaction", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22);"));

		// we start a transaction, but shutdown the DB before committing
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (22, 23);"));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("UNIQUE INDEX after shutdown", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER PRIMARY KEY, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22);"));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));

		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (11, 24)"));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("CREATE INDEX statement after shutdown", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22);"));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));

		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 24)"));

		REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON test using art(a)"));

		result = con.Query("SELECT a, b FROM test WHERE a=11 ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 11}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 24}));

		result = con.Query("SELECT a, b FROM test WHERE a>11 ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22}));

		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a=11 AND b=24"));

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));
	}
	// now with updates
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));

		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 24)"));

		REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON test using art(a)"));

		result = con.Query("SELECT a, b FROM test WHERE a=11 ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 11}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 24}));

		result = con.Query("SELECT a, b FROM test WHERE a>11 ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22}));

		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a=11 AND b=22"));
		REQUIRE_NO_FAIL(con.Query("UPDATE test SET b=22 WHERE a=11"));

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));
	}
	DeleteDatabase(storage_database);
}
