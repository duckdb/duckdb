#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Create and drop a view over different runs", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA test;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test.t (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("CREATE VIEW test.v AS SELECT * FROM test.t;"));
		result = con.Query("PRAGMA table_info('test.v')");
		REQUIRE(CHECK_COLUMN(result, 1, {"a", "b"}));
		REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER", "INTEGER"}));

		// drop the table that "test.v" is based on
		REQUIRE_NO_FAIL(con.Query("DROP TABLE test.t"));

		// we can still query the types and column names
		result = con.Query("PRAGMA table_info('test.v')");
		REQUIRE(CHECK_COLUMN(result, 1, {"a", "b"}));
		REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER", "INTEGER"}));

		// but querying the view fails!
		REQUIRE_FAIL(con.Query("SELECT * FROM test.v"));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 3; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		// the table is not there yet, but we can still check the view types
		result = con.Query("PRAGMA table_info('test.v')");
		REQUIRE(CHECK_COLUMN(result, 1, {"a", "b"}));
		REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER", "INTEGER"}));
		REQUIRE_FAIL(con.Query("SELECT * FROM test.v"));

		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test.t (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test.t"));
		// after creating the table we can query the view again
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test.v"));
		result = con.Query("PRAGMA table_info('test.v')");
		REQUIRE(CHECK_COLUMN(result, 1, {"a", "b"}));
		REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER", "INTEGER"}));
		REQUIRE_NO_FAIL(con.Query("DROP TABLE test.t"));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test views with explicit column aliases", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA test;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test.t (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("CREATE VIEW test.v (b,c) AS SELECT * FROM test.t;"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test.v"));
		REQUIRE_NO_FAIL(con.Query("DROP TABLE test.t"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test.t (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test.t"));
		REQUIRE_NO_FAIL(con.Query("SELECT b,c FROM test.v"));
		REQUIRE_NO_FAIL(con.Query("DROP TABLE test.t"));
	}
	// reload again
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test.t (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test.t"));
		REQUIRE_NO_FAIL(con.Query("SELECT b,c FROM test.v"));
		REQUIRE_NO_FAIL(con.Query("DROP VIEW test.v"));
	}
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test.t"));
		REQUIRE_FAIL(con.Query("SELECT b,c FROM test.v"));
	}
	DeleteDatabase(storage_database);
}
