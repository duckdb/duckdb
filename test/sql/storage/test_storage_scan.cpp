#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scanning of persisted storage", "[storage]") {
	auto config = GetTestConfig();
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11), (12), (13), (14), (15), (NULL)"));
	}
	// perform read-only scans a few times
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 12, 13, 14, 15}));
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 12, 13, 14, 15}));
	}
	// now perform a deletion
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 12, 13, 14, 15}));
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 12, 13, 14, 15}));

		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a=12;"));

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 13, 14, 15}));
	}
	// reload and perform another deletion
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 13, 14, 15}));

		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a=13;"));

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 14, 15}));
	}
	// reload and read again
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 14, 15}));
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 14, 15}));
	}
	DeleteDatabase(storage_database);
}
