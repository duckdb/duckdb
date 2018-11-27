
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test UNION/EXCEPT/INTERSECT", "[setop]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	result = con.Query("SELECT 1 UNION ALL SELECT 2");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 2, 'b'");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "b"}));

	result = con.Query(
	    "SELECT 1, 'a' UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "b", "c"}));

	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 2, 'b' UNION ALL SELECT "
	                   "3, 'c' UNION ALL SELECT 4, 'd'");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "b", "c", "d"}));

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
	                   "UNION SELECT 1, 'a' ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "b", "c"}));

	result = con.Query("SELECT b FROM test WHERE a < 13 UNION  SELECT b FROM "
	                   "test WHERE a > 11");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

	// mixed fun
	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 1, 'a' UNION SELECT 2, "
	                   "'b' UNION SELECT 1, 'a' ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a", "a", "b"}));

	result = con.Query("SELECT 1, 'a' UNION ALL SELECT 1, 'a' UNION SELECT 2, "
	                   "'b' UNION SELECT 1, 'a' ORDER BY 1 DESC");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"b", "a", "a"}));
}

TEST_CASE("Test UNION in subquery with aliases", "[setop]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (42)"));

	result = con.Query("select i, j from (select i, 1 as j from t group by i "
	                   "union all select i, 2 as j from t group by i) sq1");

	REQUIRE(CHECK_COLUMN(result, 0, {42, 42}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
}

TEST_CASE("Test EXCEPT / INTERSECT", "[setop]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (41), (42), (43)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE b(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO b VALUES (40), (43), (43)"));

	result = con.Query("select * from a except select * from b");
	REQUIRE(CHECK_COLUMN(result, 0, {41, 42}));

	result = con.Query("select * from a intersect select * from b");
	REQUIRE(CHECK_COLUMN(result, 0, {43}));
}
