#include "catch.hpp"
#include "common/file_system.hpp"
#include "test_helpers.hpp"
#include "storage/storage_info.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scanning a table and computing an aggregate over a table that exceeds buffer manager size", "[storage][.]") {
	unique_ptr<MaterializedQueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// set the maximum memory to 10MB
	config->maximum_memory = 10000000;

	int64_t expected_sum;
	Value sum;
	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21), (NULL, NULL)"));
		uint64_t table_size = 2 * 4 * sizeof(int);
		uint64_t desired_size = 10 * config->maximum_memory;
		expected_sum = 11 + 12 + 13 + 22 + 22 + 21;
		// grow the table until it exceeds 100MB
		while (table_size < desired_size) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT * FROM test"));
			table_size *= 2;
			expected_sum *= 2;
		}
		sum = Value::BIGINT(expected_sum);
		// compute the sum
		result = con.Query("SELECT SUM(a) + SUM(b) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {sum}));
	}
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT SUM(a) + SUM(b) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {sum}));
	}
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT SUM(a) + SUM(b) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {sum}));
	}
	DeleteDatabase(storage_database);
}