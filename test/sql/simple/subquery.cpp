
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test subqueries", "[subqueries]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// scalar subquery
	result = con.Query("SELECT (SELECT 42)");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// nested subquery
	result = con.Query("SELECT (SELECT (SELECT 42))");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	result = con.Query("INSERT INTO test VALUES (11, 22)");
	result = con.Query("INSERT INTO test VALUES (12, 21)");
	result = con.Query("INSERT INTO test VALUES (13, 22)");

	// select single tuple only in scalar subquery
	result = con.Query("SELECT (SELECT a * 42 FROM test)");
	REQUIRE(CHECK_COLUMN(result, 0, {11 * 42}));

	// operations on subquery
	result = con.Query("SELECT a*(SELECT 42) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {11 * 42, 12 * 42, 13 * 42}));

	result = con.Query("CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d "
	                   "INTEGER, e INTEGER)");
	result = con.Query("INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)");
	result = con.Query("INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105)");

	result = con.Query("SELECT c-(SELECT sum(c) FROM t1) FROM t1");
	REQUIRE(CHECK_COLUMN(result, 0, {102 - 208, 106 - 208}));

	result = con.Query("SELECT CASE WHEN c>(SELECT sum(c)/count(*) FROM t1) "
	                   "THEN a*2 ELSE b*10 END FROM t1");
	REQUIRE(CHECK_COLUMN(result, 0, {1000, 214}));
	// correlated subqueries
	result = con.Query("SELECT a, (SELECT SUM(b) FROM test tsub WHERE "
	                   "test.a=tsub.a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 21, 22}));

	result = con.Query("SELECT a, (SELECT CASE WHEN test.a=11 THEN 22 ELSE "
	                   "NULL END) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, Value(), Value()}));

	result = con.Query("SELECT a, (SELECT CASE WHEN test.a=11 THEN b ELSE NULL "
	                   "END FROM test tsub) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, Value(), Value()}));

	result = con.Query("SELECT * from test where a=(SELECT MIN(a) FROM test t "
	                   "WHERE t.b=test.b)");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 21}));

	// exists / in / any subqueries
	result = con.Query("SELECT * FROM test WHERE EXISTS (SELECT a FROM test ts "
	                   "WHERE ts.a = test.a AND b>21)");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));
}

TEST_CASE("Test subqueries with (NOT) IN clause", "[subqueries]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (id INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 22)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (2, 21)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3, 23)"));

	result = con.Query("SELECT * FROM test WHERE b IN (SELECT b FROM test "
	                   "WHERE b * id < 30) ORDER BY id, b");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {22}));

	result = con.Query("SELECT * FROM test WHERE b NOT IN (SELECT b FROM test "
	                   "WHERE b * id < 30) ORDER BY id, b");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {21, 23}));
}

TEST_CASE("Test correlated subqueries in WHERE clause", "[subqueries]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (id INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 22)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 21)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (2, 22)"));

	result = con.Query("SELECT * FROM test WHERE b=(SELECT MIN(b) FROM test AS "
	                   "a WHERE a.id=test.id)");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {21, 22}));
}

TEST_CASE("Joins in subqueries", "[subqueries]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test (id INTEGER, test_value INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 22)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 21)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (2, 22)"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test2 (id INTEGER, test2_value INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 44)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (2, 42)"));

	result = con.Query(
	    "SELECT * FROM test, test2 WHERE test.id=test2.id AND "
	    "test_value*test2_value=(SELECT MIN(test_value*test2_value) FROM test "
	    "AS a, test2 WHERE a.id=test.id AND a.id=test2.id)");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {44, 42}));
}
