#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Create and drop a view over different runs", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = FileSystem::JoinPath(TESTING_DIRECTORY_NAME, "storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA test;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test.t (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("CREATE VIEW test.v AS SELECT * FROM test.t;"));
		REQUIRE_NO_FAIL(con.Query("DROP TABLE test.t"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test.t (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test.t"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test.v"));
		REQUIRE_NO_FAIL(con.Query("DROP TABLE test.t"));
	}
	// reload again
	{
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test.t (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test.t"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test.v"));
		REQUIRE_NO_FAIL(con.Query("DROP VIEW test.v"));
	}
	{
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test.t"));
		REQUIRE_FAIL(con.Query("SELECT * FROM test.v"));
	}
	DeleteDatabase(storage_database);
}
