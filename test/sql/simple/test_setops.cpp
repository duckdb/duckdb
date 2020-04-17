#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test UNION/EXCEPT/INTERSECT", "[setop]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	result = con.Query("SELECT 1 UNION ALL SELECT 2");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 2, 'b'");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "b"}));

	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 2, 'b' UNION ALL SELECT "
	                   "3, 'c' order by 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "b", "c"}));

	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 2, 'b' UNION ALL SELECT "
	                   "3, 'c' UNION ALL SELECT 4, 'd' order by 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "b", "c", "d"}));

	// UNION/EXCEPT/INTERSECT with NULL values
	result = con.Query("SELECT NULL UNION SELECT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT NULL EXCEPT SELECT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT NULL INTERSECT SELECT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// create tables
	result = con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	result = con.Query("INSERT INTO test VALUES (11, 1), (12, 1), (13, 2)");

	// UNION ALL, no unique results
	result = con.Query("SELECT a FROM test WHERE a < 13 UNION ALL SELECT a "
	                   "FROM test WHERE a = 13");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));

	result = con.Query("SELECT b FROM test WHERE a < 13 UNION ALL SELECT b "
	                   "FROM test WHERE a > 11");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 2}));

	// mixing types, should upcast
	result = con.Query("SELECT 1 UNION ALL SELECT 'asdf'");
	REQUIRE(CHECK_COLUMN(result, 0, {"1", "asdf"}));

	result = con.Query("SELECT NULL UNION ALL SELECT 'asdf'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "asdf"}));

	// only UNION, distinct results

	result = con.Query("SELECT 1 UNION SELECT 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT 1, 'a' UNION SELECT 2, 'b' UNION SELECT 3, 'c' "
	                   "UNION SELECT 1, 'a' order by 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "b", "c"}));

	result = con.Query("SELECT b FROM test WHERE a < 13 UNION  SELECT b FROM "
	                   "test WHERE a > 11 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

	// mixed fun
	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 1, 'a' UNION SELECT 2, "
	                   "'b' UNION SELECT 1, 'a' ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "b"}));

	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 1, 'a' UNION SELECT 2, "
	                   "'b' UNION SELECT 1, 'a' ORDER BY 1 DESC");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"b", "a"}));
}

TEST_CASE("Test UNION in subquery with aliases", "[setop]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (42)"));

	result = con.Query("select i, j from (select i, 1 as j from t group by i "
	                   "union all select i, 2 as j from t group by i) sq1");

	REQUIRE(CHECK_COLUMN(result, 0, {42, 42}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
}

TEST_CASE("Test EXCEPT / INTERSECT", "[setop]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (41), (42), (43)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE b(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO b VALUES (40), (43), (43)"));

	result = con.Query("select * from a except select * from b order by 1");
	REQUIRE(CHECK_COLUMN(result, 0, {41, 42}));

	result = con.Query("select * from a intersect select * from b");
	REQUIRE(CHECK_COLUMN(result, 0, {43}));
}

TEST_CASE("Test nested EXCEPT", "[setop]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("create table a (i integer)"));
	REQUIRE_NO_FAIL(con.Query("create table b(i integer)"));
	REQUIRE_NO_FAIL(con.Query("create table c (i integer)"));

	REQUIRE_NO_FAIL(con.Query("insert into a values(42), (43), (44)"));
	REQUIRE_NO_FAIL(con.Query("insert into b values(43)"));
	REQUIRE_NO_FAIL(con.Query("insert into c values(44)"));

	result = con.Query("select * from a except select * from b except select * from c");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Test UNION type casting", "[setop]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// type casting in single union
	result = con.Query("SELECT 1 UNION SELECT 1.0");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// type casting in nested union
	result = con.Query("SELECT 1 UNION (SELECT 1.0 UNION SELECT 1.0 UNION SELECT 1.0) UNION SELECT 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT 1 UNION (SELECT '1' UNION SELECT '1' UNION SELECT '1') UNION SELECT 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {"1"}));
}
