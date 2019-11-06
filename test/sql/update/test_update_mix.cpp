#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test mix of updates inserts and deletes", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1), (2), (3);"));

	result = con.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(6)}));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	// append from con2
	REQUIRE_NO_FAIL(con2.Query("INSERT INTO test VALUES (4), (5), (6);"));

	result = con.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(6)}));
	result = con2.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(21)}));

	// delete from con2
	REQUIRE_NO_FAIL(con2.Query("DELETE FROM test WHERE a < 4"));

	result = con.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(6)}));
	result = con2.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(15)}));

	// update from con2
	REQUIRE_NO_FAIL(con2.Query("UPDATE test SET a=a-3"));

	result = con.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(6)}));
	result = con2.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(6)}));

	// now commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(6)}));
	result = con2.Query("SELECT SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(6)}));
}

TEST_CASE("Test update and delete of the same tuple", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// on a normal table, we can update and delete the same tuple concurrently without a conflict
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1), (2), (3);"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION;"));

	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=a+1;"));
	REQUIRE_NO_FAIL(con2.Query("DELETE FROM test"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	REQUIRE_NO_FAIL(con.Query("COMMIT;"));
	REQUIRE_NO_FAIL(con2.Query("COMMIT;"));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));
}
