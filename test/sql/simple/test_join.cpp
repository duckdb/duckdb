#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test basic joins of tables", "[joins]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

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
	SECTION("ambiguous reference to column") {
		REQUIRE_FAIL(con.Query("SELECT b FROM test, test2 WHERE test.b > test2.b;"));
	}
	SECTION("simple cross product + multiple join conditions") {
		result = con.Query("SELECT a, test.b, c FROM test, test2 WHERE test.b=test2.b AND test.a-1=test2.c");
		REQUIRE(CHECK_COLUMN(result, 0, {11}));
		REQUIRE(CHECK_COLUMN(result, 1, {1}));
		REQUIRE(CHECK_COLUMN(result, 2, {10}));
	}
	SECTION("use join columns in subquery") {
		result = con.Query("SELECT a, (SELECT test.a), c FROM test, test2 WHERE "
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

	SECTION("explicit join with only condition that is no left-right comparision") {
		result = con.Query("SELECT a, test.b, c FROM test INNER JOIN test2 ON "
		                   "test.b = 2 ORDER BY c;");
		REQUIRE(CHECK_COLUMN(result, 0, {12, 12, 12}));
		REQUIRE(CHECK_COLUMN(result, 1, {2, 2, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {10, 20, 30}));
	}
	SECTION("explicit join with only condition that is constant") {
		result = con.Query("SELECT a, test.b, c FROM test INNER JOIN test2 ON NULL = 2;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		REQUIRE(CHECK_COLUMN(result, 1, {}));
		REQUIRE(CHECK_COLUMN(result, 2, {}));
	}

	SECTION("equality join where both lhs and rhs keys are projected") {
		result = con.Query("SELECT * FROM (VALUES (1)) tbl(i) JOIN (VALUES (1)) tbl2(j) ON (i=j);");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		REQUIRE(CHECK_COLUMN(result, 1, {1}));
	}
	SECTION("equality join where both lhs and rhs keys are projected with filter") {
		result =
		    con.Query("SELECT * FROM (VALUES (1), (2)) tbl(i) JOIN (VALUES (1), (2)) tbl2(j) ON (i=j) WHERE i+j=2;");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		REQUIRE(CHECK_COLUMN(result, 1, {1}));
	}
}

TEST_CASE("Test join with > STANDARD_VECTOR_SIZE duplicates", "[joins][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	size_t element_count = STANDARD_VECTOR_SIZE * 10;
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (b INTEGER, c INTEGER);"));
	for (size_t i = 0; i < element_count; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 10)"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT COUNT(*) FROM test2;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(element_count)}));

	result = con.Query("SELECT COUNT(*) FROM test INNER JOIN test2 ON test.b=test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(element_count)}));
}

TEST_CASE("Equality + inequality joins", "[joins]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (a INTEGER, c INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (11, 1), (12, 1), (13, 4)"));

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
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER, str VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 1, 'a'), (12, 2, 'b'), (13, 3, 'c')"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (a INTEGER, c INTEGER, str2 VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (11, 1, 'd'), (12, 1, 'e'), (13, 4, 'f')"));

	result = con.Query("SELECT * FROM test WHERE EXISTS(SELECT * FROM test2 "
	                   "WHERE test.a=test2.a AND test.b<>test2.c);");
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {"b", "c"}));

	result = con.Query("SELECT * FROM test WHERE EXISTS(SELECT * FROM test2 WHERE "
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

	result = con.Query("SELECT * FROM test WHERE NOT EXISTS(SELECT * FROM test2 WHERE "
	                   "test.a=test2.a AND test.b<test2.c) AND NOT EXISTS(SELECT * FROM test2 "
	                   "WHERE test.a=test2.a AND test.b>test2.c);");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {"a"}));

	result = con.Query("SELECT * FROM test WHERE EXISTS(SELECT * FROM test2 WHERE "
	                   "test.a=test2.a AND test.b<>test2.c) AND test.a > 11;");
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {"b", "c"}));
}

TEST_CASE("Equality + inequality anti and semi joins with selection vector", "[joins]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER, str VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 1, 'a'), (12, 2, 'b'), (13, 3, 'c')"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (a INTEGER, c INTEGER, str2 VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (11, 1, 'd'), (12, 1, 'e'), (13, 4, 'f')"));

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

	result = con.Query("SELECT * FROM test WHERE EXISTS(SELECT * FROM test2 WHERE "
	                   "test.a=test2.a AND test.b<>test2.c) AND test.a < 13;");
	REQUIRE(CHECK_COLUMN(result, 0, {12}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	REQUIRE(CHECK_COLUMN(result, 2, {"b"}));
}

TEST_CASE("Test range joins", "[joins]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	// create tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (b INTEGER, c INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 10), (1, 20), (2, 30)"));

	result = con.Query("SELECT test.b, test2.b FROM test, test2 WHERE test.b<test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));

	result = con.Query("SELECT test.b, test2.b FROM test, test2 WHERE test.b "
	                   "<= test2.b ORDER BY 1,2");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 2, 2}));

	// range join on multiple predicates
	result = con.Query(
	    "SELECT test.a, test.b, test2.b, test2.c FROM test, test2 WHERE test.a>test2.c AND test.b <= test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {10}));

	// introduce some NULL values
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, NULL), (NULL, 1)"));
	// join result should be unchanged
	result = con.Query(
	    "SELECT test.a, test.b, test2.b, test2.c FROM test, test2 WHERE test.a>test2.c AND test.b <= test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {10}));

	// on the RHS as well
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, NULL), (NULL, 10)"));
	// join result should be unchanged
	result = con.Query(
	    "SELECT test.a, test.b, test2.b, test2.c FROM test, test2 WHERE test.a>test2.c AND test.b <= test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {10}));
}

TEST_CASE("Test inequality joins", "[joins]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	// create tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (b INTEGER, c INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 10), (1, 20), (2, 30)"));

	// inequality join
	result = con.Query("SELECT test.b, test2.b FROM test, test2 WHERE test.b <> test2.b ORDER BY test.b, test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 2, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 1, 1, 1, 1, 2}));
	// inequality join with filter
	result = con.Query("SELECT test.b, test2.b FROM test, test2 WHERE test.b <> test2.b AND test.b <> 1 AND test2.b <> "
	                   "2 ORDER BY test.b, test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 2, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 1, 1}));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (NULL, NULL)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (NULL, NULL)"));
	// inequality join with NULL values
	result = con.Query("SELECT test.b, test2.b FROM test, test2 WHERE test.b <> test2.b ORDER BY test.b, test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 2, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 1, 1, 1, 1, 2}));

	// inequality join with filter and NULL values
	result = con.Query("SELECT test.b, test2.b FROM test, test2 WHERE test.b <> test2.b AND test.b <> 1 AND test2.b <> "
	                   "2 ORDER BY test.b, test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 2, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 1, 1}));
}

TEST_CASE("Test USING joins", "[joins]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	// create tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (1,2,3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t2 (a INTEGER, b INTEGER, c INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t2 VALUES (1,2,3), (2,2,4), (1,3,4)"));

	// USING join
	result = con.Query("SELECT t2.a, t2.b, t2.c FROM t1 JOIN t2 USING(a) ORDER BY t2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {3, 4}));

	result = con.Query("SELECT t2.a, t2.b, t2.c FROM t1 JOIN t2 USING(b) ORDER BY t2.c");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {3, 4}));

	result = con.Query("SELECT t2.a, t2.b, t2.c FROM t1 JOIN t2 USING(a,b)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	REQUIRE(CHECK_COLUMN(result, 2, {3}));

	result = con.Query("SELECT t2.a, t2.b, t2.c FROM t1 JOIN t2 USING(a,b,c)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	REQUIRE(CHECK_COLUMN(result, 2, {3}));

	// USING columns can be used without requiring a table specifier
	result = con.Query("SELECT a+1 FROM t1 JOIN t2 USING(a) ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 2}));

	REQUIRE_FAIL(con.Query("SELECT t2.a, t2.b, t2.c FROM t1 JOIN t2 USING(a+b)"));
	REQUIRE_FAIL(con.Query("SELECT t2.a, t2.b, t2.c FROM t1 JOIN t2 USING(\"\")"));
	REQUIRE_FAIL(con.Query("SELECT t2.a, t2.b, t2.c FROM t1 JOIN t2 USING(d)"));
	REQUIRE_FAIL(con.Query("SELECT t2.a, t2.b, t2.c FROM t1 JOIN t2 USING(t1.a)"));

	result = con.Query("SELECT * FROM t1 JOIN t2 USING(a,b)");
	REQUIRE(result->names.size() == 4);
	REQUIRE(result->names[0] == "a");
	REQUIRE(result->names[1] == "b");
	REQUIRE(result->names[2] == "c");
	REQUIRE(result->names[3] == "c");

	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	REQUIRE(CHECK_COLUMN(result, 2, {3}));
	REQUIRE(CHECK_COLUMN(result, 3, {3}));

	// CONTROVERSIAL:
	// we do not allow this because it is ambiguous: "b" can be bind to both "t1.b" or "t2.b" and this would give
	// different results SQLite allows this, PostgreSQL does not
	REQUIRE_FAIL(con.Query("SELECT * FROM t1 JOIN t2 USING(a) JOIN t2 t2b USING (b);"));
	// a chain with the same column name is allowed though!
	result = con.Query("SELECT * FROM t1 JOIN t2 USING(a) JOIN t2 t2b USING (a) ORDER BY 1, 2, 3, 4, 5, 6, 7");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2, 2, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {3, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {2, 2, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 4, {3, 3, 4, 4}));
	REQUIRE(CHECK_COLUMN(result, 5, {2, 3, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 6, {3, 4, 3, 4}));

	REQUIRE(result->names.size() == 7);
	REQUIRE(result->names[0] == "a");
	REQUIRE(result->names[1] == "b");
	REQUIRE(result->names[2] == "c");
	REQUIRE(result->names[3] == "b");
	REQUIRE(result->names[4] == "c");
	REQUIRE(result->names[5] == "b");
	REQUIRE(result->names[6] == "c");
}

TEST_CASE("Test chaining USING joins", "[joins]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1 (a INTEGER, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (1, 2)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t2 (b INTEGER, c INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t2 VALUES (2, 3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t3 (c INTEGER, d INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t3 VALUES (3, 4)"));

	// multiple joins with using
	// single column
	result = con.Query("SELECT * FROM t1 JOIN t2 USING (b) JOIN t3 USING (c) ORDER BY 1, 2, 3, 4;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	REQUIRE(CHECK_COLUMN(result, 2, {3}));
	REQUIRE(CHECK_COLUMN(result, 3, {4}));

	REQUIRE(result->names.size() == 4);
	REQUIRE(result->names[0] == "a");
	REQUIRE(result->names[1] == "b");
	REQUIRE(result->names[2] == "c");
	REQUIRE(result->names[3] == "d");

	// column does not exist on left side of join
	REQUIRE_FAIL(con.Query("SELECT * FROM t1 JOIN t2 USING (c)"));
	// column does not exist on right side of join
	REQUIRE_FAIL(con.Query("SELECT * FROM t1 JOIN t2 USING (a)"));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE t1"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE t2"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE t3"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (1, 2, 2)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t2 (b INTEGER, c INTEGER, d INTEGER, e INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t2 VALUES (2, 2, 3, 4)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t3 (d INTEGER, e INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t3 VALUES (3, 4)"));

	// multi column
	result = con.Query("SELECT * FROM t1 JOIN t2 USING (b, c) JOIN t3 USING (d, e);");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	REQUIRE(CHECK_COLUMN(result, 2, {2}));
	REQUIRE(CHECK_COLUMN(result, 3, {3}));
	REQUIRE(CHECK_COLUMN(result, 4, {4}));

	REQUIRE(result->names.size() == 5);
	REQUIRE(result->names[0] == "a");
	REQUIRE(result->names[1] == "b");
	REQUIRE(result->names[2] == "c");
	REQUIRE(result->names[3] == "d");
	REQUIRE(result->names[4] == "e");
}

TEST_CASE("Test joins with various columns that are only used in the join", "[joins]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	// create tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (b INTEGER, c INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 10), (1, 20), (2, 30)"));

	// count of single join
	result = con.Query("SELECT COUNT(*) FROM test, test2 WHERE test.b = test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	// now a sum
	result = con.Query("SELECT SUM(test.a), MIN(test.a), MAX(test.a) FROM test, test2 WHERE test.b = test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {34}));
	REQUIRE(CHECK_COLUMN(result, 1, {11}));
	REQUIRE(CHECK_COLUMN(result, 2, {12}));

	// count of multi-way join
	result = con.Query("SELECT COUNT(*) FROM test a1, test a2, test a3 WHERE a1.b=a2.b AND a2.b=a3.b");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	// now a sum
	result = con.Query("SELECT SUM(a1.a) FROM test a1, test a2, test a3 WHERE a1.b=a2.b AND a2.b=a3.b");
	REQUIRE(CHECK_COLUMN(result, 0, {36}));

	// count of multi-way join with filters
	result = con.Query("SELECT COUNT(*) FROM test a1, test a2, test a3 WHERE a1.b=a2.b AND a2.b=a3.b AND a1.a=11 AND "
	                   "a2.a=11 AND a3.a=11");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// unused columns that become unused because of optimizer
	result = con.Query("SELECT (TRUE OR a1.a=a2.b) FROM test a1, test a2 WHERE a1.a=11 AND a2.a>=10");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true}));
}

TEST_CASE("Test joins with comparisons involving both sides of the join", "[joins]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	// create tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (4, 1), (2, 2)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (b INTEGER, c INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (1, 2), (3, 0)"));

	result = con.Query("SELECT * FROM test JOIN test2 ON test.a+test2.c=test.b+test2.b");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {3}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));

	result = con.Query("SELECT * FROM test LEFT JOIN test2 ON test.a+test2.c=test.b+test2.b ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), 0}));

	result = con.Query("SELECT * FROM test RIGHT JOIN test2 ON test.a+test2.c=test.b+test2.b ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {2, 0}));
}
