#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test storage of default values", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER DEFAULT 1, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (11)"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		REQUIRE(CHECK_COLUMN(result, 1, {11}));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (12), (13)"));
		result = con.Query("SELECT * FROM test ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1}));
		REQUIRE(CHECK_COLUMN(result, 1, {11, 12, 13}));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1}));
		REQUIRE(CHECK_COLUMN(result, 1, {11, 12, 13}));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (14), (15)"));
		result = con.Query("SELECT * FROM test ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1, 1}));
		REQUIRE(CHECK_COLUMN(result, 1, {11, 12, 13, 14, 15}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test storage of default values with sequences", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER DEFAULT nextval('seq'), b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (11)"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		REQUIRE(CHECK_COLUMN(result, 1, {11}));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (12), (13)"));
		result = con.Query("SELECT * FROM test ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 1, {11, 12, 13}));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 1, {11, 12, 13}));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test (b) VALUES (14), (15)"));
		result = con.Query("SELECT * FROM test ORDER BY b");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
		REQUIRE(CHECK_COLUMN(result, 1, {11, 12, 13, 14, 15}));
	}
	DeleteDatabase(storage_database);
}
