#include "catch.hpp"
#include "test_helpers.hpp"

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
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	for(index_t i = 0; i < count; i++) {
		sum += i;
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (?)", (int) i));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));
	result = con2.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));

	// increment all even values by two
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=a+2 WHERE a%2=0"));

	result = con.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum + count)}));
	result = con2.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));

	// now increment all odd values by two
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=a+2 WHERE a%2=1"));

	result = con.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum + count * 2)}));
	result = con2.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));

	// increment all tuples by two now
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=a+2"));

	result = con.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum + count * 4)}));
	result = con2.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));

	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum + count * 4)}));
	result = con2.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum + count * 4)}));
}
