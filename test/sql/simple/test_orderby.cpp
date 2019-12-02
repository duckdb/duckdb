#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test ORDER BY keyword", "[order]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (12, 21), (13, 22);"));

	// simple ORDER BY
	result = con.Query("SELECT b FROM test ORDER BY a DESC;");
	REQUIRE(CHECK_COLUMN(result, 0, {22, 21, 22}));
	REQUIRE(result->types.size() == 1);

	result = con.Query("SELECT a, b FROM test ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 21, 22}));

	result = con.Query("SELECT a, b FROM test ORDER BY a DESC;");
	REQUIRE(CHECK_COLUMN(result, 0, {13, 12, 11}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 21, 22}));

	// ORDER BY on multiple columns
	result = con.Query("SELECT a, b FROM test ORDER BY b, a;");
	REQUIRE(CHECK_COLUMN(result, 0, {12, 11, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {21, 22, 22}));

	// ORDER BY using select indices
	result = con.Query("SELECT a, b FROM test ORDER BY 2, 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {12, 11, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {21, 22, 22}));

	result = con.Query("SELECT a, b FROM test ORDER BY b DESC, a;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 13, 12}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 22, 21}));

	result = con.Query("SELECT a, b FROM test ORDER BY b, a DESC;");
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13, 11}));
	REQUIRE(CHECK_COLUMN(result, 1, {21, 22, 22}));

	// TOP N queries
	result = con.Query("SELECT a, b FROM test ORDER BY b, a DESC LIMIT 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {12}));
	REQUIRE(CHECK_COLUMN(result, 1, {21}));

	// Offset
	result = con.Query("SELECT a, b FROM test ORDER BY b, a DESC LIMIT 1 OFFSET 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22}));

	// Offset without limit
	result = con.Query("SELECT a, b FROM test ORDER BY b, a DESC OFFSET 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {13, 11}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));

	result = con.Query("SELECT a, b FROM test WHERE a < 13 ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {12, 11}));
	REQUIRE(CHECK_COLUMN(result, 1, {21, 22}));

	result = con.Query("SELECT a, b FROM test WHERE a < 13 ORDER BY 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {12, 11}));
	REQUIRE(CHECK_COLUMN(result, 1, {21, 22}));

	result = con.Query("SELECT a, b FROM test WHERE a < 13 ORDER BY b DESC;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 21}));

	result = con.Query("SELECT b, a FROM test WHERE a < 13 ORDER BY b DESC;");
	REQUIRE(CHECK_COLUMN(result, 0, {22, 21}));
	REQUIRE(CHECK_COLUMN(result, 1, {11, 12}));

	// order by expression
	result = con.Query("SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f ORDER BY b % 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {24, 12}));

	// order by expression that is not in SELECT
	result = con.Query("SELECT b % 2 AS f, a FROM test ORDER BY b % 4;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 0, 0}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 11, 13}));

	// ORDER BY alias
	result = con.Query("SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f ORDER BY f;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {24, 12}));

	result = con.Query("SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {24, 12}));

	// ORDER BY after union
	result = con.Query("SELECT a-10 AS k FROM test UNION SELECT a-10 AS l FROM test ORDER BY k;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// ORDER BY on alias in right-most query
	// CONTROVERSIAL: SQLite allows both "k" and "l" to be referenced here, Postgres and MonetDB give an error.
	result = con.Query("SELECT a-10 AS k FROM test UNION SELECT a-10 AS l FROM test ORDER BY l;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// computations with aliases are not allowed though
	REQUIRE_FAIL(con.Query("SELECT a-10 AS k FROM test UNION SELECT a-10 AS l FROM test ORDER BY 1-k;"));

	// but ordering on computation elements should work
	result = con.Query("SELECT a-10 AS k FROM test UNION SELECT a-10 AS l FROM test ORDER BY a-10;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	result = con.Query("SELECT a-10 AS k FROM test UNION SELECT a-11 AS l FROM test ORDER BY a-11;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3}));
}

TEST_CASE("Test ORDER BY exceptions", "[order]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (12, 21), (13, 22);"));

	// ORDER BY index out of range
	REQUIRE_FAIL(con.Query("SELECT a FROM test ORDER BY 2"));

	// ORDER BY constant works, but does nothing
	// CONTROVERSIAL: works in SQLite but not in Postgres
	result = con.Query("SELECT a FROM test ORDER BY 'hello', a");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));

	// ambiguous reference in union alias
	REQUIRE_FAIL(con.Query("SELECT a AS k, b FROM test UNION SELECT a, b AS k FROM test ORDER BY k"));

	// but works if not ambiguous
	result = con.Query("SELECT a AS k, b FROM test UNION SELECT a AS k, b FROM test ORDER BY k");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {22, 21, 22}));

	// ambiguous reference in union parameter
	REQUIRE_FAIL(con.Query("SELECT a % 2, b FROM test UNION SELECT b, a % 2 AS k ORDER BY a % 2"));

	// but works if not ambiguous
	result = con.Query("SELECT a % 2, b FROM test UNION SELECT a % 2 AS k, b FROM test ORDER BY a % 2");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {21, 22}));

	// out of range order also happens for unions
	REQUIRE_FAIL(con.Query("SELECT a % 2, b FROM test UNION SELECT a % 2 AS k, b FROM test ORDER BY 3"));
	REQUIRE_FAIL(con.Query("SELECT a % 2, b FROM test UNION SELECT a % 2 AS k, b FROM test ORDER BY -1"));

	// and union itself fails if amount of entries is wrong
	REQUIRE_FAIL(con.Query("SELECT a % 2, b FROM test UNION SELECT a % 2 AS k FROM test ORDER BY -1"));
}

TEST_CASE("Test ORDER BY with large table", "[order][.]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	for (size_t i = 0; i < 10000; i++) {
		con.Query("INSERT INTO test VALUES (" + to_string(10000 - i) + ")");
	}
	result = con.Query("SELECT * FROM test ORDER BY a");
	for (size_t i = 0; i < 10000; i++) {
		REQUIRE(result->GetValue<int32_t>(0, i) == i + 1);
	}
}

TEST_CASE("Test Top N Optimization", "[order]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (22), (2), (7);"));

	// Top N optimization
	result = con.Query("SELECT b FROM test ORDER BY b DESC LIMIT 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {22, 7}));

	// Top N optimization: works with OFFSET
	result = con.Query("SELECT b FROM test ORDER BY b LIMIT 1 OFFSET 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {7}));

	// Top N optimization: Limit greater than number of rows
	result = con.Query("SELECT b FROM test ORDER BY b LIMIT 10 OFFSET 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {7, 22}));

	// Top N optimization: Offset greater than total number of rows
	result = con.Query("SELECT b FROM test ORDER BY b LIMIT 10 OFFSET 10;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	// Top N optimization: doesn't apply for Offset without Limit
	result = con.Query("SELECT b FROM test ORDER BY b OFFSET 10;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
}
