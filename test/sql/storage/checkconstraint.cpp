
#include "catch.hpp"
#include "test_helpers.hpp"

#include "common/file_system.hpp"
#include "dbgen.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test serialization of CHECK constraint", "[storage]") {
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
		REQUIRE_NO_FAIL(con.Query(
		    "CREATE TABLE test(a INTEGER CHECK (a<10), b INTEGER CHECK(CASE "
		    "WHEN b < 10 THEN a < b ELSE a + b < 100 END));"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		DuckDBConnection con(db);
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
	RemoveDirectory(storage_database);
}
