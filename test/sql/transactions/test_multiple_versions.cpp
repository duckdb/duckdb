#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/storage/storage_info.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test multiple versions of the same data", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);
	con.EnableQueryVerification();

	// initialize the database
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3);"));

	// we can query the database using both connections
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	result = con2.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));

	// now update the database in connection 1
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=5 WHERE i=1;"));
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));
	// con 2 still has the same result
	result = con2.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));

	// we can update the same data point again in con 1
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=10 WHERE i=5;"));
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {15}));
	// con 2 still has the same result
	result = con2.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));

	// now delete it
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i>5;"));
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	// con 2 still has the same result
	result = con2.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));

	// insert some new data again
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2)"));
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {8}));
	// con 2 still has the same result
	result = con2.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));

	// now commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	// con 2 now has the updated results
	result = con2.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {8}));
}

TEST_CASE("Test multiple versions of the same data with a data set that exceeds a single block", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);
	con.EnableQueryVerification();

	// set up the database
	uint64_t integer_count = 2 * (Storage::BLOCK_SIZE / sizeof(int32_t));
	uint64_t current_count = 4;
	uint64_t expected_sum = 10;
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (4);"));
	while (current_count < integer_count) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers SELECT * FROM integers"));
		current_count *= 2;
		expected_sum *= 2;
	}
	// verify the count and sum
	result = con.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(current_count)}));
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum)}));

	for (idx_t i = 1; i <= 4; i++) {
		// now delete some tuples
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=" + to_string(i)));

		// check the updated count and sum
		result = con.Query("SELECT COUNT(*) FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(current_count - (current_count / 4))}));
		result = con.Query("SELECT SUM(i) FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum - i * (current_count / 4))}));

		// con2 still has the same count and sum
		result = con2.Query("SELECT COUNT(*) FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(current_count)}));
		result = con2.Query("SELECT SUM(i) FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum)}));

		// rollback
		REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));

		// now the count and sum are back to normal
		result = con.Query("SELECT COUNT(*) FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(current_count)}));
		result = con.Query("SELECT SUM(i) FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum)}));
	}
}
