#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test simple storage", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = FileSystem::JoinPath(TESTING_DIRECTORY_NAME, "storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 21, 22}));
	}
	// reload the database from disk, we do this again because checkpointing at startup causes this to follow a
	// different code path
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 21, 22}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test storing NULLs and strings", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = FileSystem::JoinPath(TESTING_DIRECTORY_NAME, "storage_test");

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
	auto storage_database = FileSystem::JoinPath(TESTING_DIRECTORY_NAME, "storage_test");

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

TEST_CASE("Test storing TPC-H", "[storage][.]") {
	unique_ptr<QueryResult> result;
	double sf = 0.1;
	auto storage_database = FileSystem::JoinPath(TESTING_DIRECTORY_NAME, "storage_tpch");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert TPC-H tables
		DuckDB db(storage_database);
		// generate the TPC-H data for SF 0.1
		tpch::dbgen(sf, db);
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		// check if all the counts are correct
		result = con.Query("SELECT COUNT(*) FROM orders");
		REQUIRE(CHECK_COLUMN(result, 0, {150000}));
		result = con.Query("SELECT COUNT(*) FROM lineitem");
		REQUIRE(CHECK_COLUMN(result, 0, {600572}));
		result = con.Query("SELECT COUNT(*) FROM part");
		REQUIRE(CHECK_COLUMN(result, 0, {20000}));
		result = con.Query("SELECT COUNT(*) FROM partsupp");
		REQUIRE(CHECK_COLUMN(result, 0, {80000}));
		result = con.Query("SELECT COUNT(*) FROM supplier");
		REQUIRE(CHECK_COLUMN(result, 0, {1000}));
		result = con.Query("SELECT COUNT(*) FROM customer");
		REQUIRE(CHECK_COLUMN(result, 0, {15000}));
		result = con.Query("SELECT COUNT(*) FROM nation");
		REQUIRE(CHECK_COLUMN(result, 0, {25}));
		result = con.Query("SELECT COUNT(*) FROM region");
		REQUIRE(CHECK_COLUMN(result, 0, {5}));
	}
	// reload the database from disk again
	{
		DuckDB db(storage_database);
		Connection con(db);
		// check if all the counts are correct
		result = con.Query("SELECT COUNT(*) FROM orders");
		REQUIRE(CHECK_COLUMN(result, 0, {150000}));
		result = con.Query("SELECT COUNT(*) FROM lineitem");
		REQUIRE(CHECK_COLUMN(result, 0, {600572}));
		result = con.Query("SELECT COUNT(*) FROM part");
		REQUIRE(CHECK_COLUMN(result, 0, {20000}));
		result = con.Query("SELECT COUNT(*) FROM partsupp");
		REQUIRE(CHECK_COLUMN(result, 0, {80000}));
		result = con.Query("SELECT COUNT(*) FROM supplier");
		REQUIRE(CHECK_COLUMN(result, 0, {1000}));
		result = con.Query("SELECT COUNT(*) FROM customer");
		REQUIRE(CHECK_COLUMN(result, 0, {15000}));
		result = con.Query("SELECT COUNT(*) FROM nation");
		REQUIRE(CHECK_COLUMN(result, 0, {25}));
		result = con.Query("SELECT COUNT(*) FROM region");
		REQUIRE(CHECK_COLUMN(result, 0, {5}));
	}
	// reload the database from disk again
	{
		DuckDB db(storage_database);
		Connection con(db);
		// check if all the counts are correct
		result = con.Query("SELECT COUNT(*) FROM orders");
		REQUIRE(CHECK_COLUMN(result, 0, {150000}));
		result = con.Query("SELECT COUNT(*) FROM lineitem");
		REQUIRE(CHECK_COLUMN(result, 0, {600572}));
		result = con.Query("SELECT COUNT(*) FROM part");
		REQUIRE(CHECK_COLUMN(result, 0, {20000}));
		result = con.Query("SELECT COUNT(*) FROM partsupp");
		REQUIRE(CHECK_COLUMN(result, 0, {80000}));
		result = con.Query("SELECT COUNT(*) FROM supplier");
		REQUIRE(CHECK_COLUMN(result, 0, {1000}));
		result = con.Query("SELECT COUNT(*) FROM customer");
		REQUIRE(CHECK_COLUMN(result, 0, {15000}));
		result = con.Query("SELECT COUNT(*) FROM nation");
		REQUIRE(CHECK_COLUMN(result, 0, {25}));
		result = con.Query("SELECT COUNT(*) FROM region");
		REQUIRE(CHECK_COLUMN(result, 0, {5}));
	}
	DeleteDatabase(storage_database);
}
