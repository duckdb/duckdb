#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test binding parameters with union expressions", "[setops]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(b INTEGER);"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1), (2), (3), (NULL);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (2), (3), (4), (NULL);"));

	result = con.Query("(SELECT a FROM test ORDER BY a+1) UNION SELECT b FROM test2 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	REQUIRE(result->types.size() == 1);

	// union returns only one column
	result = con.Query("SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test2) res ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	REQUIRE(result->types.size() == 1);
	// we can only bind by the column names of the first query
	result = con.Query("SELECT a FROM (SELECT * FROM test UNION SELECT * FROM test2) res ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	REQUIRE(result->types.size() == 1);
	// hence this does not work: "b" is from the second query
	REQUIRE_FAIL(con.Query("SELECT b FROM (SELECT * FROM test UNION SELECT * FROM test2) res ORDER BY 1;"));
	// it works if we reverse the tables
	result = con.Query("SELECT b FROM (SELECT * FROM test2 UNION SELECT * FROM test) res ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	REQUIRE(result->types.size() == 1);
	// we can give explicit subquery aliases
	result = con.Query("SELECT col1 FROM (SELECT * FROM test2 UNION SELECT * FROM test) res(col1) ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	REQUIRE(result->types.size() == 1);

	// we can ORDER BY names from both sides
	result = con.Query("SELECT a FROM test UNION SELECT b FROM test2 ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	REQUIRE(result->types.size() == 1);
	result = con.Query("SELECT a FROM test UNION SELECT b FROM test2 ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	REQUIRE(result->types.size() == 1);
	// if names are ambiguous, throw an error
	REQUIRE_FAIL(con.Query("SELECT 1, a FROM test UNION SELECT b AS a, 1 FROM test2 ORDER BY a;"));
	// if expressions are ambiguous as well, throw an error
	REQUIRE_FAIL(con.Query("SELECT 1, a+1 FROM test UNION SELECT a+1, 1 FROM test ORDER BY a+1;"));
	// also if we have multiple setops
	result = con.Query("SELECT a FROM test UNION SELECT b FROM test2 UNION SELECT b AS c FROM test2 ORDER BY c;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	REQUIRE(result->types.size() == 1);
	// we can also order by the expression itself
	result = con.Query("SELECT a FROM test UNION SELECT b FROM test2 UNION SELECT b + 1 FROM test2 ORDER BY b + 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4, 5}));
	REQUIRE(result->types.size() == 1);
	// multiple columns order
	result = con.Query("SELECT a, 10 - a AS b FROM test UNION SELECT b, b + 1 FROM test2 ORDER BY 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3, 4, 3, 2, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 4, 5, 7, 8, 9}));
	// ambiguous naming reference should fail
	REQUIRE_FAIL(con.Query("SELECT a, 10 - a AS b FROM test UNION SELECT b, b + 1 FROM test2 ORDER BY b;"));
	// and by constant references
	result = con.Query("SELECT a FROM test UNION SELECT b FROM test2 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	REQUIRE(result->types.size() == 1);
	// out of range constant reference
	REQUIRE_FAIL(con.Query("SELECT a FROM test UNION SELECT b FROM test2 ORDER BY 2;"));

	// what if our subqueries have an order by clause?
	result = con.Query("(SELECT a FROM test ORDER BY a+1) UNION SELECT b FROM test2 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	REQUIRE(result->types.size() == 1);

	// unions with SELECT * also allows orders
	result = con.Query("SELECT * FROM test UNION SELECT * FROM test2 ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	result = con.Query("SELECT * FROM test UNION SELECT * FROM test2 ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));

	// test union with/without table specifiers
	result = con.Query("SELECT a FROM test UNION SELECT * FROM test2 ORDER BY test.a;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	result = con.Query("SELECT a FROM test UNION SELECT b FROM test2 ORDER BY test2.b;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	result = con.Query("SELECT test.a FROM test UNION SELECT * FROM test2 ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	result = con.Query("SELECT test.a FROM test UNION SELECT test2.b FROM test2 ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));

	// what about multiple set ops?
	result = con.Query(
	    "SELECT a FROM test UNION SELECT * FROM test2 UNION SELECT * FROM test t1 ORDER BY test.a, test2.b, t1.a;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	result = con.Query("SELECT a FROM test UNION SELECT * FROM test2 UNION SELECT * FROM test t1 ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	// and subqueries
	result = con.Query("SELECT a FROM (SELECT * FROM test) bla UNION SELECT * FROM test2 ORDER BY bla.a;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4}));
	// what if we have cross products or joins
	result = con.Query("SELECT t1.a, t2.a FROM test t1, test t2 WHERE t1.a=t2.a UNION SELECT b, b - 1 FROM test2 ORDER "
	                   "BY t1.a, t2.a, test2.b;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 2, 3, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 1, 2, 2, 3, 3}));
}

TEST_CASE("Test union with nulls", "[setops]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	result = con.Query("SELECT NULL as a, NULL as b, 1 as id UNION SELECT CAST('2015-10-11 00:00:00' AS TIMESTAMP) as "
	                   "a, CAST('2015-10-11 12:34:56' AS TIMESTAMP) as b, 2 as id ORDER BY 3");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value::TIMESTAMP(2015, 10, 11, 0, 0, 0, 0)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value::TIMESTAMP(2015, 10, 11, 12, 34, 56, 0)}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2}));
}
