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
}
