#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"
#include "duckdb/storage/storage_info.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test abort of commit with persistent storage", "[storage]") {
	unique_ptr<MaterializedQueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	Value expected_count, expected_count_b;
	Value expected_sum_a, expected_sum_b, expected_sum_strlen;

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db), con2(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER PRIMARY KEY, b INTEGER, c VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query(
		    "INSERT INTO test VALUES (11, 22, 'hello'), (13, 22, 'world'), (12, 21, 'test'), (10, NULL, NULL)"));

		// start a transaction for con and con2
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

		// insert the value 14 in both transactions
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (14, 10, 'con')"));
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO test VALUES (15, 10, 'con2')"));
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO test VALUES (14, 10, 'con2')"));

		// commit both
		// con2 will fail
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
		REQUIRE_FAIL(con2.Query("COMMIT"));

		// now add another row of all NULLs
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (15, NULL, NULL)"));

		// check that the result is correct
		expected_count = Value::BIGINT(6);
		expected_count_b = Value::BIGINT(4);
		expected_sum_a = Value::BIGINT(10 + 11 + 12 + 13 + 14 + 15);
		expected_sum_b = Value::BIGINT(22 + 22 + 21 + 10);
		expected_sum_strlen = Value::BIGINT(5 + 5 + 4 + 3);

		// verify the contents of the database
		result = con.Query("SELECT COUNT(*), COUNT(a), COUNT(b), SUM(a), SUM(b), SUM(LENGTH(c)) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {expected_count}));
		REQUIRE(CHECK_COLUMN(result, 1, {expected_count}));
		REQUIRE(CHECK_COLUMN(result, 2, {expected_count_b}));
		REQUIRE(CHECK_COLUMN(result, 3, {expected_sum_a}));
		REQUIRE(CHECK_COLUMN(result, 4, {expected_sum_b}));
		REQUIRE(CHECK_COLUMN(result, 5, {expected_sum_strlen}));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT COUNT(*), COUNT(a), COUNT(b), SUM(a), SUM(b), SUM(LENGTH(c)) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {expected_count}));
		REQUIRE(CHECK_COLUMN(result, 1, {expected_count}));
		REQUIRE(CHECK_COLUMN(result, 2, {expected_count_b}));
		REQUIRE(CHECK_COLUMN(result, 3, {expected_sum_a}));
		REQUIRE(CHECK_COLUMN(result, 4, {expected_sum_b}));
		REQUIRE(CHECK_COLUMN(result, 5, {expected_sum_strlen}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test abort of large commit with persistent storage", "[storage][.]") {
	unique_ptr<MaterializedQueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	Value expected_count, expected_count_b;
	Value expected_sum_a, expected_sum_b, expected_sum_strlen;

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db), con2(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER PRIMARY KEY, b INTEGER, c VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query(
		    "INSERT INTO test VALUES (11, 22, 'hello'), (13, 22, 'world'), (12, 21, 'test'), (10, NULL, NULL)"));

		// start a transaction for con and con2
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

		// insert the value 14 in con
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (14, 10, 'con')"));
		// now insert many non-conflicting values in con2
		idx_t tpl_count = 2 * Storage::BLOCK_SIZE / sizeof(int);
		for (int i = 0; i < (int)tpl_count; i++) {
			REQUIRE_NO_FAIL(con2.Query("INSERT INTO test VALUES (" + to_string(15 + i) + ", 10, 'con2')"));
		}
		// finally insert one conflicting tuple
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO test VALUES (14, 10, 'con2')"));

		// commit both
		// con2 will fail
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
		REQUIRE_FAIL(con2.Query("COMMIT"));

		// now add another row of all NULLs
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (15, NULL, NULL)"));

		// check that the result is correct
		expected_count = Value::BIGINT(6);
		expected_count_b = Value::BIGINT(4);
		expected_sum_a = Value::BIGINT(10 + 11 + 12 + 13 + 14 + 15);
		expected_sum_b = Value::BIGINT(22 + 22 + 21 + 10);
		expected_sum_strlen = Value::BIGINT(5 + 5 + 4 + 3);

		// verify the contents of the database
		result = con.Query("SELECT COUNT(*), COUNT(a), COUNT(b), SUM(a), SUM(b), SUM(LENGTH(c)) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {expected_count}));
		REQUIRE(CHECK_COLUMN(result, 1, {expected_count}));
		REQUIRE(CHECK_COLUMN(result, 2, {expected_count_b}));
		REQUIRE(CHECK_COLUMN(result, 3, {expected_sum_a}));
		REQUIRE(CHECK_COLUMN(result, 4, {expected_sum_b}));
		REQUIRE(CHECK_COLUMN(result, 5, {expected_sum_strlen}));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT COUNT(*), COUNT(a), COUNT(b), SUM(a), SUM(b), SUM(LENGTH(c)) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {expected_count}));
		REQUIRE(CHECK_COLUMN(result, 1, {expected_count}));
		REQUIRE(CHECK_COLUMN(result, 2, {expected_count_b}));
		REQUIRE(CHECK_COLUMN(result, 3, {expected_sum_a}));
		REQUIRE(CHECK_COLUMN(result, 4, {expected_sum_b}));
		REQUIRE(CHECK_COLUMN(result, 5, {expected_sum_strlen}));
	}
	DeleteDatabase(storage_database);
}
