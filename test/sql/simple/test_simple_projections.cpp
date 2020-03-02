#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test simple projection statements", "[simpleprojection]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create table
	result = con.Query("CREATE TABLE a (i integer, j integer);");

	// insertion: 1 affected row
	result = con.Query("INSERT INTO a VALUES (42, 84);");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT * FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {84}));

	// multiple insertions
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (12, 21), (13, 22)"));

	// multiple projections
	result = con.Query("SELECT a, b FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 21, 22}));

	// basic expressions and filters
	result = con.Query("SELECT a + 2, b FROM test WHERE a = 11;");
	REQUIRE(CHECK_COLUMN(result, 0, {13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22}));

	result = con.Query("SELECT a + 2, b FROM test WHERE a = 12;");
	REQUIRE(CHECK_COLUMN(result, 0, {14}));
	REQUIRE(CHECK_COLUMN(result, 1, {21}));

	// casts
	result = con.Query("SELECT cast(a AS VARCHAR) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {"11", "12", "13"}));

	result = con.Query("SELECT cast(cast(a AS VARCHAR) as INTEGER) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
}

TEST_CASE("Test table star expressions", "[simpleprojection]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (12, 21), (13, 22)"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 21, 22}));

	result = con.Query("SELECT test.* FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 21, 22}));

	result = con.Query("SELECT t.* FROM test t");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 21, 22}));

	REQUIRE_FAIL(con.Query("SELECT test.* FROM test t"));
	REQUIRE_FAIL(con.Query("SELECT xyz.* FROM test"));
	REQUIRE_FAIL(con.Query("SELECT xyz.*"));

	// issue 415
	REQUIRE_NO_FAIL(con.Query("create table r4 (i int, j int)"));
	REQUIRE_NO_FAIL(con.Query("insert into r4 (i, j) values (1,1), (1,2), (1,3), (1,4), (1,5)"));

	result = con.Query("select t1.i, t1.j as a, t2.j as b from r4 t1 inner join r4 t2 using(i,j) ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 4, 5}));

	result = con.Query(
	    "select t1.i, t1.j as a, t2.j as b from r4 t1 inner join r4 t2 on t1.i=t2.i and t1.j=t2.j ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 4, 5}));

	result = con.Query("select t1.*, t2.j b from r4 t1 inner join r4 t2 using(i,j) ORDER BY t1.j");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 4, 5}));

	result = con.Query("select t1.*, t2.j b from r4 t1 inner join r4 t2 on t1.i=t2.i and t1.j=t2.j ORDER BY t1.j");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 4, 5}));
}
