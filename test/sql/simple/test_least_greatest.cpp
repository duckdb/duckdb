#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test LEAST/GREATEST", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// scalar usage
	result = con.Query("SELECT LEAST(1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT LEAST(1, 3)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT LEAST(1, 3, 0)");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT LEAST(1, 3, 0, 2, 7, 8, 10, 11, -100, 30)");
	REQUIRE(CHECK_COLUMN(result, 0, {-100}));
	result = con.Query("SELECT LEAST(1, 3, 0, 2, 7, 8, 10, 11, -100, 30, NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT LEAST(NULL, 3, 0, 2, 7, 8, 10, 11, -100, 30, 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// double
	result = con.Query("SELECT LEAST(1.0, 10.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {1.0}));

	// strings
	result = con.Query("SELECT LEAST('hello', 'world')");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	result = con.Query("SELECT LEAST('hello', 'world', 'blabla', 'tree')");
	REQUIRE(CHECK_COLUMN(result, 0, {"blabla"}));
	result = con.Query("SELECT LEAST('hello', 'world', 'blabla', 'tree')");
	REQUIRE(CHECK_COLUMN(result, 0, {"blabla"}));

	// dates
	result = con.Query("SELECT LEAST(DATE '1992-01-01', DATE '1994-02-02', DATE '1991-01-01')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1991, 1, 1)}));
	result = con.Query("SELECT LEAST(DATE '1992-01-01', DATE '1994-02-02', DATE '1991-01-01', NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// test mix of types
	result = con.Query("SELECT LEAST(DATE '1992-01-01', 'hello', 123)");
	REQUIRE(CHECK_COLUMN(result, 0, {"123"}));

	// tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (1, NULL), (2, 1), (3, 7)"));

	result = con.Query("SELECT LEAST(i, j), GREATEST(i, j) FROM t1 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 7}));
	result = con.Query("SELECT LEAST(i, i + 1, j), GREATEST(i, i - 1, j) FROM t1 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 7}));
	result = con.Query("SELECT LEAST(i, 800, i + 1, 1000, j), GREATEST(i, -1000, i - 1, -700, j, -800) FROM t1 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 7}));
	result = con.Query("SELECT LEAST(i, 800, i + 1, 1000, j, NULL), GREATEST(i, -1000, i - 1, -700, j, -800) FROM t1 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 7}));

	// selection vectors
	result = con.Query("SELECT LEAST(i, j), GREATEST(i, j) FROM t1 WHERE j IS NOT NULL ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 7}));

	// row ids
	result = con.Query("SELECT LEAST(rowid + 10, i, j), GREATEST(i, rowid + 4, j) FROM t1 WHERE j IS NOT NULL ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 7}));

	// generated strings
	result = con.Query("SELECT LEAST(REPEAT(i::VARCHAR, 20), j::VARCHAR) FROM t1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "1", "33333333333333333333"}));
}
