#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test config.use_direct_io work", "[storage][.]") {
	constexpr idx_t VALUE_COUNT = 10;
	idx_t expected_sum = 0;

	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	auto config = GetTestConfig();
    config->options.use_direct_io = true;

	duckdb::unique_ptr<DuckDB> database;
	duckdb::unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("direct_io_test");


	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{

		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(a INTEGER);"));
		for (idx_t i = 0; i < VALUE_COUNT; i++) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(i) + ");"));
			expected_sum += i;
		}
		REQUIRE_NO_FAIL(con.Query("COMMIT;"));
		result = con.Query("SELECT SUM(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum)}));
	}
	// force a checkpoint by reloading
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
	}

	DeleteDatabase(storage_database);
}
