#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test aggregation/group by statements", "[aggregations]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));

	// aggregates cannot be nested
	REQUIRE_FAIL(con.Query("SELECT SUM(SUM(41)), COUNT(*);"));

	// simple aggregates without group by
	result = con.Query("SELECT SUM(a), COUNT(*), AVG(a) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {36}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {12.0}));

	result = con.Query("SELECT COUNT(*) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	result = con.Query("SELECT SUM(a), COUNT(*) FROM test WHERE a = 11;");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));

	result = con.Query("SELECT SUM(a), SUM(b), SUM(a) + SUM (b) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {36}));
	REQUIRE(CHECK_COLUMN(result, 1, {65}));
	REQUIRE(CHECK_COLUMN(result, 2, {101}));

	result = con.Query("SELECT SUM(a+2), SUM(a) + 2 * COUNT(*) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {42}));

	// aggregations with group by
	result = con.Query("SELECT b, SUM(a), SUM(a+2), AVG(a) FROM test GROUP BY b ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 24}));
	REQUIRE(CHECK_COLUMN(result, 2, {14, 28}));
	REQUIRE(CHECK_COLUMN(result, 3, {12, 12}));

	// ORDER BY aggregation that does not occur in SELECT clause
	result = con.Query("SELECT b, SUM(a) FROM test GROUP BY b ORDER BY COUNT(a);");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 24}));

	result = con.Query("SELECT b, SUM(a) FROM test GROUP BY b ORDER BY COUNT(a) DESC;");
	REQUIRE(CHECK_COLUMN(result, 0, {22, 21}));
	REQUIRE(CHECK_COLUMN(result, 1, {24, 12}));

	result = con.Query("SELECT b, SUM(a), COUNT(*), SUM(a+2) FROM test GROUP "
	                   "BY b ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 24}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {14, 28}));

	// group by alias
	result = con.Query("SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {24, 12}));

	// group by with filter
	result = con.Query("SELECT b, SUM(a), COUNT(*), SUM(a+2) FROM test WHERE "
	                   "a <= 12 GROUP "
	                   "BY b ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 11}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 1}));
	REQUIRE(CHECK_COLUMN(result, 3, {14, 13}));

	// nested aggregate in group by
	REQUIRE_FAIL(con.Query("SELECT b % 2 AS f, COUNT(SUM(a)) FROM test GROUP BY f;"));

	con.Query("INSERT INTO test VALUES (12, 21), (12, 21), (12, 21)");

	// group by with filter and multiple values per groups
	result = con.Query("SELECT b, SUM(a), COUNT(*), SUM(a+2) FROM test WHERE "
	                   "a <= 12 GROUP "
	                   "BY b ORDER BY b;");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {12 * 4, 11}));
	REQUIRE(CHECK_COLUMN(result, 2, {4, 1}));
	REQUIRE(CHECK_COLUMN(result, 3, {12 * 4 + 2 * 4, 13}));

	// group by with filter and multiple values per groups
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 4), (3, 4), (2, 4);"));

	// use GROUP BY column in math operator
	result = con.Query("SELECT i, i + 10 FROM integers GROUP BY i ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 13}));

	// using non-group column and non-aggregate should throw an error
	REQUIRE_FAIL(con.Query("SELECT i, SUM(j), j FROM integers GROUP BY i ORDER BY i"));
	// but it works if we wrap it in FIRST()
	result = con.Query("SELECT i, SUM(j), FIRST(j) FROM integers GROUP BY i ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 8}));
	REQUIRE(CHECK_COLUMN(result, 2, {4, 4}));

	// group by constant alias
	result = con.Query("SELECT 1 AS k, SUM(i) FROM integers GROUP BY k ORDER BY 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {8}));

	// use an alias that is identical to a column name (should prioritize column name)
	result = con.Query("SELECT 1 AS i, SUM(i) FROM integers GROUP BY i ORDER BY 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 6}));

	// refer to the same alias twice
	result = con.Query("SELECT i % 2 AS k, SUM(i) FROM integers GROUP BY k, k ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 6}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL);"));

	// group by NULL
	result = con.Query("SELECT i, SUM(i) FROM integers GROUP BY i ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// column reference should have preference over alias reference in grouping
	result = con.Query("SELECT i, i % 2 AS i, SUM(i) FROM integers GROUP BY i ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 0, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 1, 2, 3}));

	// aliases can only be referenced in the GROUP BY as the root column: operations not allowed
	// CONTROVERSIAL: this query DOES work in SQLite
	REQUIRE_FAIL(con.Query("SELECT 1 AS k, SUM(i) FROM integers GROUP BY k+1 ORDER BY 2;"));

	// group by column refs should be recognized, even if one uses an explicit table specifier and the other does not
	result = con.Query("SELECT test.b, SUM(a) FROM test GROUP BY b ORDER BY COUNT(a) DESC;");
	REQUIRE(CHECK_COLUMN(result, 0, {21, 22}));
	REQUIRE(CHECK_COLUMN(result, 1, {48, 24}));
}

TEST_CASE("Test aliases in group by/aggregation", "[aggregations]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// use alias in HAVING clause
	// CONTROVERSIAL: this query DOES NOT work in PostgreSQL
	result = con.Query("SELECT i % 2 AS k, SUM(i) FROM integers WHERE i IS NOT NULL GROUP BY k HAVING k>0;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {4}));

	// this is identical to this query
	// CONTROVERSIAL: this query does not work in MonetDB
	result = con.Query("SELECT i % 2 AS k, SUM(i) FROM integers WHERE i IS NOT NULL GROUP BY k HAVING i%2>0;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {4}));

	// select groups by constant (similar to order by constant)
	result = con.Query("SELECT i % 2 AS k, SUM(i) FROM integers WHERE i IS NOT NULL GROUP BY 1 HAVING i%2>0;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {4}));

	// constant out of range
	REQUIRE_FAIL(con.Query("SELECT i % 2 AS k, SUM(i) FROM integers WHERE i IS NOT NULL GROUP BY 42 HAVING i%2>0;"));

	// entry in GROUP BY should refer to base column
	// ...BUT the alias in ORDER BY should refer to the alias from the select list
	// note that both Postgres and MonetDB reject this query because of ambiguity. SQLite accepts it though so we do
	// too.
	result = con.Query("SELECT i, i % 2 AS i, SUM(i) FROM integers GROUP BY i ORDER BY i, 3;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 0, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 2, 1, 3}));

	// changing the name of the alias makes it more explicit what should happen
	result = con.Query("SELECT i, i % 2 AS k, SUM(i) FROM integers GROUP BY i ORDER BY k, 3;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 0, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 2, 1, 3}));

	// this now orders by the actual grouping column
	result = con.Query("SELECT i, i % 2 AS k, SUM(i) FROM integers GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 0, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 1, 2, 3}));

	// cannot use GROUP BY column in an aggregation...
	REQUIRE_FAIL(con.Query("SELECT i % 2 AS k, SUM(k) FROM integers GROUP BY k"));

	// ...unless it is one of the base columns
	result = con.Query("SELECT i, SUM(i) FROM integers GROUP BY i ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// ORDER on a non-grouping column
	// this query is refused by Postgres and MonetDB
	// but SQLite resolves it by first pushing a "FIRST(i)" aggregate into the projection, and then ordering by that
	// aggregate
	REQUIRE_FAIL(con.Query("SELECT (10-i) AS k, SUM(i) FROM integers GROUP BY k ORDER BY i;"));

	// we can manually get this behavior by pushing FIRST
	result = con.Query("SELECT (10-i) AS k, SUM(i) FROM integers GROUP BY k ORDER BY FIRST(i);");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 9, 8, 7}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
}

TEST_CASE("GROUP BY large strings", "[aggregations]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('helloworld', 22), "
	                          "('thisisalongstring', 22), ('helloworld', 21)"));

	result = con.Query("SELECT a, SUM(b) FROM test GROUP BY a ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"helloworld", "thisisalongstring"}));
	REQUIRE(CHECK_COLUMN(result, 1, {43, 22}));
}

TEST_CASE("Group by multiple columns", "[aggregations]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER, k INTEGER);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO integers VALUES (1, 1, 2), (1, 2, 2), (1, 1, 2), (2, 1, 2), (1, 2, 4), (1, 2, NULL);"));

	result = con.Query("SELECT i, j, SUM(k), COUNT(*), COUNT(k) FROM integers GROUP BY i, j ORDER BY 1, 2");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {4, 6, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {2, 3, 1}));
	REQUIRE(CHECK_COLUMN(result, 4, {2, 2, 1}));
}

TEST_CASE("Aggregate only COUNT STAR", "[aggregations]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 4), (3, 4), (2, 4);"));

	result = con.Query("SELECT i, COUNT(*) FROM integers GROUP BY i ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));

	// test COUNT without the *
	result = con.Query("SELECT i, COUNT() FROM integers GROUP BY i ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
}

TEST_CASE("GROUP BY NULL value", "[aggregations]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 4), (NULL, 4), (2, 4);"));

	result = con.Query("SELECT i, SUM(j) FROM integers GROUP BY i ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 4, 4}));
}

TEST_CASE("Aggregating from empty table", "[aggregations]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE emptyaggr(i INTEGER);"));

	result = con.Query("SELECT COUNT(*) FROM emptyaggr");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	result = con.Query("SELECT SUM(i), COUNT(i), COUNT(DISTINCT i), COUNT(*), AVG(i), "
	                   "COUNT(*)+1, COUNT(i)+1, MIN(i), MIN(i+1), MIN(i)+1 FROM emptyaggr");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {1}));
	REQUIRE(CHECK_COLUMN(result, 6, {1}));
	REQUIRE(CHECK_COLUMN(result, 7, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 8, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 9, {Value()}));
}

TEST_CASE("DISTINCT aggregations", "[aggregations]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE distinctagg(i INTEGER, j INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO distinctagg VALUES (1,1),(1,1),(2,2), (1,2)"));

	result = con.Query("SELECT COUNT(i), COUNT(DISTINCT i), SUM(i), "
	                   "SUM(DISTINCT i) FROM distinctagg");

	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
	REQUIRE(CHECK_COLUMN(result, 3, {3}));

	result = con.Query("SELECT COUNT(i), COUNT(DISTINCT i), SUM(i), SUM(DISTINCT i) "
	                   "FROM distinctagg GROUP BY j ORDER BY j");

	REQUIRE(CHECK_COLUMN(result, 0, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {1, 3}));
}

TEST_CASE("STDDEV aggregations", "[aggregations]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("create table stddev_test(val integer, grp integer)"));
	REQUIRE_NO_FAIL(con.Query("insert into stddev_test values (42, 1), (43, "
	                          "1), (42, 2), (1000, 2), (NULL, 1), (NULL, 3)"));

	// stddev_samp
	result = con.Query("select round(stddev_samp(val), 1) from stddev_test");
	REQUIRE(CHECK_COLUMN(result, 0, {478.8}));

	result = con.Query("select round(stddev_samp(val), 1) from stddev_test  "
	                   "where val is not null");
	REQUIRE(CHECK_COLUMN(result, 0, {478.8}));

	result = con.Query("select grp, sum(val), round(stddev_samp(val), 1), "
	                   "min(val) from stddev_test group by grp order by grp");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {85, 1042, Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {0.7, 677.4, Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {42, 42, Value()}));

	result = con.Query("select grp, sum(val), round(stddev_samp(val), 1), min(val) from "
	                   "stddev_test where val is not null group by grp order by grp");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {85, 1042}));
	REQUIRE(CHECK_COLUMN(result, 2, {0.7, 677.4}));
	REQUIRE(CHECK_COLUMN(result, 3, {42, 42}));

	// stddev_pop
	result = con.Query("select round(stddev_pop(val), 1) from stddev_test");
	REQUIRE(CHECK_COLUMN(result, 0, {414.7}));

	result = con.Query("select round(stddev_pop(val), 1) from stddev_test  "
	                   "where val is not null");
	REQUIRE(CHECK_COLUMN(result, 0, {414.7}));

	result = con.Query("select grp, sum(val), round(stddev_pop(val), 1), "
	                   "min(val) from stddev_test group by grp order by grp");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {85, 1042, Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {0.5, 479.0, Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {42, 42, Value()}));

	result = con.Query("select grp, sum(val), round(stddev_pop(val), 1), min(val) from "
	                   "stddev_test where val is not null group by grp order by grp");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {85, 1042}));
	REQUIRE(CHECK_COLUMN(result, 2, {0.5, 479.0}));
	REQUIRE(CHECK_COLUMN(result, 3, {42, 42}));

	// var_samp
	result = con.Query("select round(var_samp(val), 1) from stddev_test");
	REQUIRE(CHECK_COLUMN(result, 0, {229281.6}));

	result = con.Query("select round(var_samp(val), 1) from stddev_test  "
	                   "where val is not null");
	REQUIRE(CHECK_COLUMN(result, 0, {229281.6}));

	result = con.Query("select grp, sum(val), round(var_samp(val), 1), "
	                   "min(val) from stddev_test group by grp order by grp");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {85, 1042, Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {0.5, 458882.0, Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {42, 42, Value()}));

	result = con.Query("select grp, sum(val), round(var_samp(val), 1), min(val) from "
	                   "stddev_test where val is not null group by grp order by grp");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {85, 1042}));
	REQUIRE(CHECK_COLUMN(result, 2, {0.5, 458882.0}));
	REQUIRE(CHECK_COLUMN(result, 3, {42, 42}));

	// var_pop
	result = con.Query("select round(var_pop(val), 1) from stddev_test");
	REQUIRE(CHECK_COLUMN(result, 0, {171961.2}));

	result = con.Query("select round(var_pop(val), 1) from stddev_test  "
	                   "where val is not null");
	REQUIRE(CHECK_COLUMN(result, 0, {171961.2}));

	result = con.Query("select grp, sum(val), round(var_pop(val), 2), "
	                   "min(val) from stddev_test group by grp order by grp");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {85, 1042, Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {0.25, 229441.0, Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {42, 42, Value()}));

	result = con.Query("select grp, sum(val), round(var_pop(val), 2), min(val) from "
	                   "stddev_test where val is not null group by grp order by grp");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {85, 1042}));
	REQUIRE(CHECK_COLUMN(result, 2, {0.25, 229441.0}));
	REQUIRE(CHECK_COLUMN(result, 3, {42, 42}));
}

TEST_CASE("Test aggregations on strings", "[aggregations]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT NULL as a, NULL as b, NULL as c, NULL as d, 1 as id UNION SELECT 'Кирилл' as a, "
	                   "'Müller' as b, '我是谁' as c, 'ASCII' as d, 2 as id ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "Кирилл"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), "Müller"}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), "我是谁"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), "ASCII"}));
	REQUIRE(CHECK_COLUMN(result, 4, {1, 2}));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, s VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 'hello'), (12, 'world'), (11, NULL)"));

	// scalar aggregation on string
	result = con.Query("SELECT COUNT(*), COUNT(s) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));

	// grouped aggregation on string
	result = con.Query("SELECT a, COUNT(*), COUNT(s) FROM test GROUP BY a ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 1}));

	// group by the strings
	result = con.Query("SELECT s, SUM(a) FROM test GROUP BY s ORDER BY s;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello", "world"}));
	REQUIRE(CHECK_COLUMN(result, 1, {11, 11, 12}));

	// distinct aggregations ons tring
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 'hello'), (12, 'world')"));

	// scalar distinct
	result = con.Query("SELECT COUNT(*), COUNT(s), COUNT(DISTINCT s) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {4}));
	REQUIRE(CHECK_COLUMN(result, 2, {2}));

	// grouped distinct
	result = con.Query("SELECT a, COUNT(*), COUNT(s), COUNT(DISTINCT s) FROM test GROUP BY a ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {1, 1}));

	// now with WHERE clause
	result = con.Query(
	    "SELECT a, COUNT(*), COUNT(s), COUNT(DISTINCT s) FROM test WHERE s IS NOT NULL GROUP BY a ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {1, 1}));
}
