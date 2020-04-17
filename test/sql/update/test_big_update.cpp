#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/storage/storage_info.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Update big table of even and odd values", "[update][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table with the values [0, ..., 3K]
	int64_t sum = 0;
	int64_t count = 3000;

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR);"));
	for (int64_t i = 0; i < count; i++) {
		sum += i;
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (?, 'hello')", (int)i));
	}
	// insert a bunch more values
	while (count < Storage::BLOCK_SIZE) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT * FROM test"));
		count *= 2;
		sum *= 2;
	}

	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT SUM(a), SUM(LENGTH(b)) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count * 5)}));
	result = con2.Query("SELECT SUM(a), SUM(LENGTH(b)) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count * 5)}));

	// increment all even values by two
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=a+2, b='hellohello' WHERE a%2=0"));

	result = con.Query("SELECT SUM(a), SUM(LENGTH(b)) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum + count)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count * 10 - (count / 2 * 5))}));
	result = con2.Query("SELECT SUM(a), SUM(LENGTH(b)) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count * 5)}));

	// now increment all odd values by two
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=a+2, b='hellohello' WHERE a%2=1"));

	result = con.Query("SELECT SUM(a), SUM(LENGTH(b)) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum + count * 2)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count * 10)}));
	result = con2.Query("SELECT SUM(a), SUM(LENGTH(b)) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count * 5)}));

	// increment all tuples by two now
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=a+2, b='hellohellohellohello'"));

	result = con.Query("SELECT SUM(a), SUM(LENGTH(b)) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum + count * 4)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count * 20)}));
	result = con2.Query("SELECT SUM(a), SUM(LENGTH(b)) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count * 5)}));

	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT SUM(a), SUM(LENGTH(b)) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum + count * 4)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count * 20)}));
	result = con2.Query("SELECT SUM(a), SUM(LENGTH(b)) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum + count * 4)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count * 20)}));
}
