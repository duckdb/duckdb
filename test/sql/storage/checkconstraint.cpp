
#include "catch.hpp"
#include "test_helpers.hpp"

#include "common/file_system.hpp"
#include "dbgen.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test serialization of CHECK constraint", "[storage]") {
	return;
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
		REQUIRE_NO_FAIL(
		    con.Query("CREATE TABLE test(a INTEGER CHECK (a<10));"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		DuckDBConnection con(db);
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3);"));
		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (12);"));
	}
	RemoveDirectory(storage_database);
}
