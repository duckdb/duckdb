
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
