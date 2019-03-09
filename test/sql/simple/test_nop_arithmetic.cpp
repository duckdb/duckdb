#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test NOP arithmetic expressions", "[arithmetic]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	con.Query("CREATE TABLE test (a INTEGER, b INTEGER)");
	con.Query("INSERT INTO test VALUES (42, 10), (43, 100);");

	// a + 0
	result = con.Query("SELECT a + 0 FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));
	// 0 + a
	result = con.Query("SELECT 0 + a FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));

	// a - 0
	result = con.Query("SELECT a - 0 FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));
	// 0 - a
	result = con.Query("SELECT 0 - a FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {-42, -43}));

	// a * 1
	result = con.Query("SELECT a * 1 FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));
	// 1 * a
	result = con.Query("SELECT 1 * a FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));
	// a * 0 => 0
	result = con.Query("SELECT a * 0 FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0}));
	result = con.Query("SELECT 0 * a FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0}));

	// a / 1
	result = con.Query("SELECT a / 1 FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));
	// 1 / a
	result = con.Query("SELECT 1 / a FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0}));

	// a / 0 => NULL
	result = con.Query("SELECT a / 0 FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value()}));
	// 0 / a => 0
	result = con.Query("SELECT 0 / a FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0}));

	// test expressions involving NULL as well
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=NULL"));

	// NULL * 0 = NULL
	result = con.Query("SELECT a * 0 FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value()}));

	// 0 / NULL = NULL
	result = con.Query("SELECT 0 / a FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value()}));
}
