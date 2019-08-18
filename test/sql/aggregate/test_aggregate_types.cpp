#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar aggregates with many different types", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("SELECT MIN(TRUE)"));

	// count
	result = con.Query("SELECT COUNT(), COUNT(1), COUNT(*), COUNT(NULL), COUNT('hello'), COUNT(DATE '1992-02-02')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));
	REQUIRE(CHECK_COLUMN(result, 4, {1}));
	REQUIRE(CHECK_COLUMN(result, 5, {1}));

	REQUIRE_FAIL(con.Query("SELECT COUNT(1, 2)"));
	// sum
	result = con.Query("SELECT SUM(1), SUM(NULL), SUM(33.3)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {33.3}));

	REQUIRE_FAIL(con.Query("SELECT SUM(True)"));
	REQUIRE_FAIL(con.Query("SELECT SUM('hello')"));
	REQUIRE_FAIL(con.Query("SELECT SUM(DATE '1992-02-02')"));
	REQUIRE_FAIL(con.Query("SELECT SUM()"));
	REQUIRE_FAIL(con.Query("SELECT SUM(1, 2)"));
	// min
	result = con.Query("SELECT MIN(1), MIN(NULL), MIN(33.3), MIN('hello'), MIN(True), MIN(DATE '1992-02-02'), MIN(TIMESTAMP '2008-01-01 00:00:01')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {33.3}));
	REQUIRE(CHECK_COLUMN(result, 3, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(true)}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value::DATE(1992, 2, 2)}));
	REQUIRE(CHECK_COLUMN(result, 6, {Value::TIMESTAMP(2008, 1, 1, 0, 0, 1, 0)}));

	REQUIRE_FAIL(con.Query("SELECT MIN()"));
	REQUIRE_FAIL(con.Query("SELECT MIN(1, 2)"));
	// max
	result = con.Query("SELECT MAX(1), MAX(NULL), MAX(33.3), MAX('hello'), MAX(True), MAX(DATE '1992-02-02'), MAX(TIMESTAMP '2008-01-01 00:00:01')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {33.3}));
	REQUIRE(CHECK_COLUMN(result, 3, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(true)}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value::DATE(1992, 2, 2)}));
	REQUIRE(CHECK_COLUMN(result, 6, {Value::TIMESTAMP(2008, 1, 1, 0, 0, 1, 0)}));

	REQUIRE_FAIL(con.Query("SELECT MAX()"));
	REQUIRE_FAIL(con.Query("SELECT MAX(1, 2)"));
	// first
	result = con.Query("SELECT FIRST(1), FIRST(NULL), FIRST(33.3), FIRST('hello'), FIRST(True), FIRST(DATE '1992-02-02'), FIRST(TIMESTAMP '2008-01-01 00:00:01')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {33.3}));
	REQUIRE(CHECK_COLUMN(result, 3, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(true)}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value::DATE(1992, 2, 2)}));
	REQUIRE(CHECK_COLUMN(result, 6, {Value::TIMESTAMP(2008, 1, 1, 0, 0, 1, 0)}));

	REQUIRE_FAIL(con.Query("SELECT FIRST()"));
	REQUIRE_FAIL(con.Query("SELECT FIRST(1, 2)"));

	// avg
	result = con.Query("SELECT AVG(1), AVG(NULL), AVG(33.3)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {33.3}));

	REQUIRE_FAIL(con.Query("SELECT AVG(True)"));
	REQUIRE_FAIL(con.Query("SELECT AVG('hello')"));
	REQUIRE_FAIL(con.Query("SELECT AVG(DATE '1992-02-02')"));
	REQUIRE_FAIL(con.Query("SELECT AVG()"));
	REQUIRE_FAIL(con.Query("SELECT AVG(1, 2)"));

	// string agg
	result = con.Query("SELECT STRING_AGG('hello', ' '), STRING_AGG('hello', NULL), STRING_AGG(NULL, ' '), STRING_AGG(NULL, NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	REQUIRE_FAIL(con.Query("SELECT STRING_AGG()"));
	REQUIRE_FAIL(con.Query("SELECT STRING_AGG('hello')"));
	REQUIRE_FAIL(con.Query("SELECT STRING_AGG(1, 2, 3)"));
}

