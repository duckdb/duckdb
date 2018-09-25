
#include "catch.hpp"
#include "test_helpers.hpp"

#include "common/file_system.hpp"

using namespace duckdb;
using namespace std;

#define STORAGE_TEST_DATABASE_NAME "storage_test"

TEST_CASE("Test simple storage", "[storage]") {
	unique_ptr<DuckDBResult> result;

	// make sure the database does not exist
	if (DirectoryExists(STORAGE_TEST_DATABASE_NAME)) {
		RemoveDirectory(STORAGE_TEST_DATABASE_NAME);
	}
	{
		// create a database and insert values
		DuckDB db(STORAGE_TEST_DATABASE_NAME);
		DuckDBConnection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));
	}
	// reload the database from disk
	{
		DuckDB db(STORAGE_TEST_DATABASE_NAME);
		DuckDBConnection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		CHECK_COLUMN(result, 0, {11, 12, 13});
		CHECK_COLUMN(result, 1, {22, 21, 22});
	}
}
