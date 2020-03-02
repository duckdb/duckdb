#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test that database size does not grow after many checkpoints", "[storage][.]") {
	constexpr idx_t VALUE_COUNT = 10000;
	idx_t expected_sum = 0;

	FileSystem fs;
	auto config = GetTestConfig();
	unique_ptr<DuckDB> database;
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("dbsize_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
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

	// get the size of the database
	int64_t size;
	{
		auto handle = fs.OpenFile(storage_database, FileFlags::READ);
		size = fs.GetFileSize(*handle);
		REQUIRE(size >= 0);
	}
	// now reload the database a bunch of times, and everytime we reload update all the values
	for (idx_t i = 0; i < 20; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		// verify the current count
		result = con.Query("SELECT SUM(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum)}));
		// update the table
		REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=a+1;"));
		expected_sum += VALUE_COUNT;
		// verify the current count again
		result = con.Query("SELECT SUM(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum)}));
	}
	// get the new file size
	int64_t new_size;
	{
		auto handle = fs.OpenFile(storage_database, FileFlags::READ);
		new_size = fs.GetFileSize(*handle);
		REQUIRE(new_size >= 0);
	}
	// require that the size did not grow more than factor 3
	// we allow the database file to grow somewhat because there will be empty blocks inside the file after many
	// checkpoints however this should never be more than factor ~2.5 the original database size
	REQUIRE(new_size <= size * 3);
	DeleteDatabase(storage_database);
}
