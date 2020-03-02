#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test serialization of CHECK constraint", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(a INTEGER CHECK (a<10), b INTEGER CHECK(CASE "
		                          "WHEN b < 10 THEN a < b ELSE a + b < 100 END));"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		// matching tuple
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3, 7);"));
		// check constraint on a violated (a < 10)
		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (12, 13);"));
		// check constraint on b violated  (b < 10) => (a < b)
		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (5, 3);"));
		// check constraint on b not violated !(b < 10) => (a + b < 100)
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (9, 90);"));
		// check constraint on b violated !(b < 10) => (a + b < 100)
		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (9, 99);"));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test serialization of NOT NULL constraint", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(a INTEGER NOT NULL);"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (NULL)"));
	}
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (NULL)"));
	}
	DeleteDatabase(storage_database);
}
