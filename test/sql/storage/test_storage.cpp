#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test interleaving of insertions/updates/deletes on multiple tables", "[storage][.]") {
	auto config = GetTestConfig();
	duckdb::unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (a INTEGER, b INTEGER);"));
		idx_t test_insert = 0, test_insert2 = 0;
		for (idx_t i = 0; i < 1000; i++) {
			idx_t stage = i % 7;
			switch (stage) {
			case 0:
				for (; test_insert < i; test_insert++) {
					REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(test_insert) + ")"));
				}
				break;
			case 1:
				for (; test_insert2 < i; test_insert2++) {
					REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (" + to_string(test_insert) + ", " +
					                          to_string(test_insert) + " + 2)"));
				}
				break;
			case 2:
				REQUIRE_NO_FAIL(con.Query("UPDATE test SET a = a + 1 WHERE a % 2 = 0"));
				break;
			case 3:
				REQUIRE_NO_FAIL(con.Query("UPDATE test2 SET a = a + 1 WHERE a % 2 = 0"));
				break;
			case 4:
				REQUIRE_NO_FAIL(con.Query("UPDATE test2 SET b = b + 1 WHERE b % 2 = 0"));
				break;
			case 5:
				REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a % 5 = 0"));
				break;
			default:
				REQUIRE_NO_FAIL(con.Query("DELETE FROM test2 WHERE a % 5 = 0"));
				break;
			}
		}
		REQUIRE_NO_FAIL(con.Query("COMMIT"));

		result = con.Query("SELECT SUM(a) FROM test ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {396008}));

		result = con.Query("SELECT SUM(a), SUM(b) FROM test2 ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {403915}));
		REQUIRE(CHECK_COLUMN(result, 1, {405513}));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT SUM(a) FROM test ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {396008}));

		result = con.Query("SELECT SUM(a), SUM(b) FROM test2 ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {403915}));
		REQUIRE(CHECK_COLUMN(result, 1, {405513}));
	}
	DeleteDatabase(storage_database);
}
