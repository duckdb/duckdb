
#include "catch.hpp"
#include "test_helpers.hpp"

#include "common/file_system.hpp"
#include "dbgen.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Create and drop a table over different runs", "[storage]") {
	unique_ptr<DuckDBResult> result;
	auto storage_database = JoinPath(TESTING_DIRECTORY_NAME, "storage_test");

	// make sure the database does not exist
	if (DirectoryExists(storage_database)) {
		RemoveDirectory(storage_database);
	}
	{
		// create a database and insert values
		DuckDB db(storage_database);
		DuckDBConnection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO test VALUES (11, 22), (13, 22);"));
		REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));
		
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO test VALUES (11, 22), (13, 22);"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		DuckDBConnection con(db);
		REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));
	}
	RemoveDirectory(storage_database);
}

