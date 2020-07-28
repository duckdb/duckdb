#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;
using namespace std;
// 
	// con.Record("test/sql/join/inner/test_range_join.cpp", "Test range joins");

TEST_CASE("Test join with > STANDARD_VECTOR_SIZE duplicates", "[joins][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	size_t element_count = STANDARD_VECTOR_SIZE * 10;
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (b INTEGER, c INTEGER);"));
	Appender appender(con, "test2");
	for (size_t i = 0; i < element_count; i++) {
		appender.AppendRow(1, 10);
	}
	appender.Close();
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT COUNT(*) FROM test2;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(element_count)}));

	result = con.Query("SELECT COUNT(*) FROM test INNER JOIN test2 ON test.b=test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(element_count)}));
}

TEST_CASE("Test inequality join with > STANDARD_VECTOR_SIZE duplicates", "[joins][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	size_t element_count = STANDARD_VECTOR_SIZE * 10;
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1), (2)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (b INTEGER);"));
	Appender appender(con, "test2");
	for (size_t i = 0; i < element_count; i++) {
		appender.AppendRow(1);
	}
	appender.Close();
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT COUNT(*) FROM test2;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(element_count)}));

	result = con.Query("SELECT COUNT(*) FROM test INNER JOIN test2 ON test.b<>test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(element_count)}));
}
