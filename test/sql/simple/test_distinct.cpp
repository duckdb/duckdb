#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test DISTINCT keyword", "[distinct]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (11, 21), (11, 22)");

	result = con.Query("SELECT DISTINCT a, b FROM test ORDER BY a, b");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 11, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {21, 22, 22}));

	// FIXME: this doesn't work because "test.a" is different from "a" in the ORDER BY
	// result = con.Query("SELECT DISTINCT test.a, b FROM test ORDER BY a, b");

	result = con.Query("SELECT DISTINCT a FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));

	result = con.Query("SELECT DISTINCT b FROM test ORDER BY b");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));

	result = con.Query("SELECT DISTINCT a, SUM(B) FROM test GROUP BY a ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {65, 22}));

	result = con.Query("SELECT DISTINCT MAX(b) FROM test GROUP BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {22}));

	result = con.Query("SELECT DISTINCT CASE WHEN a > 11 THEN 11 ELSE a END FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));
}

TEST_CASE("Test DISTINCT and ORDER BY", "[distinct]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	con.Query("CREATE TABLE integers(i INTEGER);");
	con.Query("INSERT INTO integers VALUES (1), (2), (3)");

	result = con.Query("SELECT DISTINCT i%2 FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));

	// controversial: Postgres fails here with the error "with SELECT DISTINCT columns from ORDER BY must appear in the
	// SELECT clause" but SQLite succeeds
	// we also succeed here, even though it can give unintuitive results
	// this is transformed into SELECT DISTINCT(1) i % 2, i
	result = con.Query("SELECT DISTINCT i % 2 FROM integers WHERE i<3 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 0}));
	result = con.Query("SELECT DISTINCT ON (1) i % 2, i FROM integers WHERE i<3 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 0}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));

	// binding of DISTINCT with column names
	result = con.Query("SELECT DISTINCT integers.i FROM integers ORDER BY i DESC");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2, 1}));
	result = con.Query("SELECT DISTINCT i FROM integers ORDER BY integers.i DESC");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2, 1}));
	result = con.Query("SELECT DISTINCT integers.i FROM integers ORDER BY integers.i DESC");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2, 1}));
}

TEST_CASE("Test DISTINCT ON", "[distinct]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER, k INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (2, 3, 5), (4, 5, 6), (2, 7, 6)"));

	result = con.Query("SELECT DISTINCT ON (i) i, j FROM integers WHERE i <> 2");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {5}));

	result = con.Query("SELECT DISTINCT ON (1) i, j FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 5}));

	result = con.Query("SELECT DISTINCT ON (1) i, j FROM integers ORDER BY i LIMIT 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));

	result = con.Query("SELECT DISTINCT ON (1) i, j FROM integers ORDER BY i LIMIT 1 OFFSET 1");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {5}));

	result = con.Query("SELECT DISTINCT ON (2) i, j FROM integers ORDER BY 2");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 5, 7}));

	result = con.Query("SELECT DISTINCT ON (2) j, k FROM integers ORDER BY 2");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 6}));

	result = con.Query("SELECT DISTINCT ON (3) i, j, k FROM integers ORDER BY 2");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 5}));
	REQUIRE(CHECK_COLUMN(result, 2, {5, 6}));

	result = con.Query("SELECT DISTINCT ON (3) i, j, k FROM integers ORDER BY 3");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 5}));
	REQUIRE(CHECK_COLUMN(result, 2, {5, 6}));

	result = con.Query("SELECT DISTINCT ON (2) j, (SELECT i FROM integers) FROM integers ORDER BY 2");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));

	result = con.Query(
	    "SELECT DISTINCT ON (2) j, (SELECT DISTINCT ON (i) i FROM integers ORDER BY 1) FROM integers ORDER BY 2");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));

	result = con.Query("SELECT DISTINCT ON (i) i, j FROM integers ORDER BY j");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 5}));

	result = con.Query("SELECT * FROM (SELECT DISTINCT ON (i) i, j FROM integers) tbl1 WHERE i <> 2");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {5}));

	// order by a column that does not exist in the SELECT clause
	result = con.Query("SELECT DISTINCT ON (i) i, j FROM integers ORDER BY k");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 5}));
	// equivalent to this, but without projecting the k
	result = con.Query("SELECT DISTINCT ON (i) i, j, k FROM integers ORDER BY k");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 5}));
	REQUIRE(CHECK_COLUMN(result, 2, {5, 6}));

	// binding of DISTINCT ON with different column names
	result = con.Query("SELECT DISTINCT ON (integers.i) i, j FROM integers ORDER BY 1, 2");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 5}));
	result = con.Query("SELECT DISTINCT ON (i) integers.i, integers.j FROM integers ORDER BY 1, 2");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 5}));
	result = con.Query("SELECT DISTINCT ON (integers.i) integers.i, integers.j FROM integers ORDER BY i, j");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 5}));

	// out of bounds
	REQUIRE_FAIL(con.Query("SELECT DISTINCT ON (2) i FROM integers"));
}
