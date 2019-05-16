#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test empty startup", "[storage]") {
	unique_ptr<DuckDB> db;
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	// create a database and close it
	REQUIRE_NOTHROW(db = make_unique<DuckDB>(storage_database));
	db.reset();
	// reload the database
	REQUIRE_NOTHROW(db = make_unique<DuckDB>(storage_database));
	db.reset();
	DeleteDatabase(storage_database);
}

TEST_CASE("Test simple storage", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21), (NULL, NULL)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (a INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (13), (12), (11)"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 22, 21, 22}));
		result = con.Query("SELECT * FROM test2 ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	}
	// reload the database from disk, we do this again because checkpointing at startup causes this to follow a
	// different code path
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 22, 21, 22}));
		result = con.Query("SELECT * FROM test2 ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test storing NULLs and strings", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b STRING);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (NULL, 'hello'), "
		                          "(13, 'abcdefgh'), (12, NULL)"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {"hello", Value(), "abcdefgh"}));
	}
	// reload the database from disk, we do this again because checkpointing at startup causes this to follow a
	// different code path
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {"hello", Value(), "abcdefgh"}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test updates with storage", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));
		for (size_t i = 0; i < 1000; i++) {
			REQUIRE_NO_FAIL(con.Query("UPDATE test SET b=b+1 WHERE a=11"));
		}
		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a=12"));
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {1022, 22}));
	}
	// reload the database from disk again
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {1022, 22}));
	}
	DeleteDatabase(storage_database);
}
