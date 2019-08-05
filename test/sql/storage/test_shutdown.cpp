#include "catch.hpp"
#include "common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Shutdown with running transaction", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	DBConfig config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, &config);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22);"));

		// we start a transaction, but shutdown the DB before committing
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (22, 23);"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database, &config);
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
	DBConfig config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, &config);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER PRIMARY KEY, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22);"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database, &config);
		Connection con(db);

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));

		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (11, 24)"));
	}
	DeleteDatabase(storage_database);
}
