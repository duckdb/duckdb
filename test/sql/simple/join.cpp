
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test basic joins of tables", "[joins]") {
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	unique_ptr<DuckDBResult> result;

	// create tables
	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)");

	con.Query("CREATE TABLE test2 (b INTEGER, c INTEGER);");
	con.Query("INSERT INTO test2 VALUES (1, 10), (1, 20), (2, 30)");

	SECTION("simple cross product + join condition") {
		result = con.Query("SELECT a, test.b, c FROM test, test2 WHERE test.b "
		                   "= test2.b ORDER BY c;");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 11, 12}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {10, 20, 30}));
	}
	SECTION("use join columns in subquery") {
		result =
		    con.Query("SELECT a, (SELECT test.a), c FROM test, test2 WHERE "
		              "test.b = test2.b ORDER BY c;");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 11, 12}));
		REQUIRE(CHECK_COLUMN(result, 1, {11, 11, 12}));
		REQUIRE(CHECK_COLUMN(result, 2, {10, 20, 30}));
	}
	SECTION("explicit join") {
		result = con.Query("SELECT a, test.b, c FROM test INNER JOIN test2 ON "
		                   "test.b = test2.b ORDER BY c;");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 11, 12}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {10, 20, 30}));
	}
	SECTION("explicit join with condition the wrong way around") {
		result = con.Query("SELECT a, test.b, c FROM test INNER JOIN test2 ON "
		                   "test2.b = test.b ORDER BY c;");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 11, 12}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {10, 20, 30}));
	}
	SECTION("explicit join with additional condition that is no left-right "
	        "comparision") {
		result = con.Query("SELECT a, test.b, c FROM test INNER JOIN test2 ON "
		                   "test2.b = test.b and test.b = 2;");
		REQUIRE(CHECK_COLUMN(result, 0, {12}));
		REQUIRE(CHECK_COLUMN(result, 1, {2}));
		REQUIRE(CHECK_COLUMN(result, 2, {30}));
	}

	SECTION("explicit join with additional condition that is constant") {
		result = con.Query("SELECT a, test.b, c FROM test INNER JOIN test2 ON "
		                   "test2.b = test.b and 2 = 2 ORDER BY c;");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 11, 12}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {10, 20, 30}));
	}

	SECTION(
	    "explicit join with only condition that is no left-right comparision") {
		result = con.Query("SELECT a, test.b, c FROM test INNER JOIN test2 ON "
		                   "test.b = 2 ORDER BY c;");
		REQUIRE(CHECK_COLUMN(result, 0, {12, 12, 12}));
		REQUIRE(CHECK_COLUMN(result, 1, {2, 2, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {10, 20, 30}));
	}
	SECTION("explicit join with only condition that is constant") {
		result = con.Query(
		    "SELECT a, test.b, c FROM test INNER JOIN test2 ON NULL = 2;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		REQUIRE(CHECK_COLUMN(result, 1, {}));
		REQUIRE(CHECK_COLUMN(result, 2, {}));
	}
}

TEST_CASE("Test join with > STANDARD_VECTOR_SIZE duplicates", "[joins]") {
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	unique_ptr<DuckDBResult> result;

	size_t element_count = STANDARD_VECTOR_SIZE * 10;
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (b INTEGER, c INTEGER);"));
	for (size_t i = 0; i < element_count; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 10)"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT COUNT(*) FROM test2;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(element_count)}));

	result = con.Query(
	    "SELECT COUNT(*) FROM test INNER JOIN test2 ON test.b=test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(element_count)}));
}

TEST_CASE("Equality + inequality joins", "[joins]") {
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	unique_ptr<DuckDBResult> result;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (a INTEGER, c INTEGER);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO test2 VALUES (11, 1), (12, 1), (13, 4)"));

	result = con.Query("SELECT test.a, b, c FROM test, test2 WHERE test.a = "
	                   "test2.a AND test.b <> test2.c ORDER BY test.a;");
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 4}));

	result = con.Query("SELECT test.a, b, c FROM test, test2 WHERE test.a = "
	                   "test2.a AND test.b < test2.c ORDER BY test.a;");
	REQUIRE(CHECK_COLUMN(result, 0, {13}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {4}));

	result = con.Query("SELECT test.a, b, c FROM test, test2 WHERE test.a = "
	                   "test2.a AND test.b <= test2.c ORDER BY test.a;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 4}));

	result = con.Query("SELECT test.a, b, c FROM test, test2 WHERE test.a = "
	                   "test2.a AND test.b > test2.c ORDER BY test.a;");
	REQUIRE(CHECK_COLUMN(result, 0, {12}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));

	result = con.Query("SELECT test.a, b, c FROM test, test2 WHERE test.a = "
	                   "test2.a AND test.b >= test2.c ORDER BY test.a;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 1}));
}

TEST_CASE("Equality + inequality anti and semi joins", "[joins]") {
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	unique_ptr<DuckDBResult> result;

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test (a INTEGER, b INTEGER, str VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO test VALUES (11, 1, 'a'), (12, 2, 'b'), (13, 3, 'c')"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test2 (a INTEGER, c INTEGER, str2 VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO test2 VALUES (11, 1, 'd'), (12, 1, 'e'), (13, 4, 'f')"));

	result = con.Query("SELECT * FROM test WHERE EXISTS(SELECT * FROM test2 "
	                   "WHERE test.a=test2.a AND test.b<>test2.c);");
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {"b", "c"}));

	result =
	    con.Query("SELECT * FROM test WHERE EXISTS(SELECT * FROM test2 WHERE "
	              "test.a=test2.a AND test.b<>test2.c) AND NOT EXISTS(SELECT * "
	              "FROM test2 WHERE test.a=test2.a AND test.b<test2.c);");
	REQUIRE(CHECK_COLUMN(result, 0, {12}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	REQUIRE(CHECK_COLUMN(result, 2, {"b"}));

	result = con.Query("SELECT * FROM test WHERE NOT EXISTS(SELECT * FROM "
	                   "test2 WHERE test.a=test2.a AND test.b<test2.c);");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {"a", "b"}));

	result = con.Query(
	    "SELECT * FROM test WHERE NOT EXISTS(SELECT * FROM test2 WHERE "
	    "test.a=test2.a AND test.b<test2.c) AND NOT EXISTS(SELECT * FROM test2 "
	    "WHERE test.a=test2.a AND test.b>test2.c);");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {"a"}));

	result =
	    con.Query("SELECT * FROM test WHERE EXISTS(SELECT * FROM test2 WHERE "
	              "test.a=test2.a AND test.b<>test2.c) AND test.a > 11;");
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {"b", "c"}));
}

TEST_CASE("Equality + inequality anti and semi joins with selection vector",
          "[joins]") {
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	unique_ptr<DuckDBResult> result;

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test (a INTEGER, b INTEGER, str VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO test VALUES (11, 1, 'a'), (12, 2, 'b'), (13, 3, 'c')"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test2 (a INTEGER, c INTEGER, str2 VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO test2 VALUES (11, 1, 'd'), (12, 1, 'e'), (13, 4, 'f')"));

	result = con.Query("SELECT * FROM test WHERE NOT EXISTS(SELECT * FROM "
	                   "test2 WHERE test.a=test2.a AND test.b<test2.c AND "
	                   "test2.a>14) AND NOT EXISTS(SELECT * FROM test2 WHERE "
	                   "test.a=test2.a AND test.b>test2.c AND test2.a<10);");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {"a", "b", "c"}));

	result = con.Query("SELECT * FROM test WHERE NOT EXISTS(SELECT * FROM "
	                   "test2 WHERE test.a=test2.a AND test.b<test2.c AND "
	                   "test2.a=12) AND NOT EXISTS(SELECT * FROM test2 WHERE "
	                   "test.a=test2.a AND test.b>test2.c AND test2.a=12);");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {"a", "c"}));

	result =
	    con.Query("SELECT * FROM test WHERE EXISTS(SELECT * FROM test2 WHERE "
	              "test.a=test2.a AND test.b<>test2.c) AND test.a < 13;");
	REQUIRE(CHECK_COLUMN(result, 0, {12}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	REQUIRE(CHECK_COLUMN(result, 2, {"b"}));
}

TEST_CASE("Test range joins", "[joins]") {
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	unique_ptr<DuckDBResult> result;

	// create tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (b INTEGER, c INTEGER);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO test2 VALUES (1, 10), (1, 20), (2, 30)"));

	result = con.Query(
	    "SELECT test.b, test2.b FROM test, test2 WHERE test.b<test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));

	result = con.Query("SELECT test.b, test2.b FROM test, test2 WHERE test.b "
	                   "<= test2.b ORDER BY 1,2");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 2, 2}));
}
