#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test correlated subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// scalar select with correlation
	result = con.Query("SELECT i, (SELECT 42+i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 43, 44, 45}));
	// ORDER BY correlated subquery
	result = con.Query("SELECT i FROM integers i1 ORDER BY (SELECT 100-i1.i);");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 3, 2, 1}));
	// subquery returning multiple results
	result = con.Query("SELECT i, (SELECT 42+i1.i FROM integers) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 43, 44, 45}));
	// subquery with LIMIT
	result = con.Query("SELECT i, (SELECT 42+i1.i FROM integers LIMIT 1) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 43, 44, 45}));
	// subquery with LIMIT 0
	result = con.Query("SELECT i, (SELECT 42+i1.i FROM integers LIMIT 0) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), Value()}));
	// subquery with WHERE clause that is always FALSE
	result = con.Query("SELECT i, (SELECT i FROM integers WHERE 1=0 AND i1.i=i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), Value()}));
	// correlated EXISTS with WHERE clause that is always FALSE
	result =
	    con.Query("SELECT i, EXISTS(SELECT i FROM integers WHERE 1=0 AND i1.i=i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, false}));
	// correlated ANY with WHERE clause that is always FALSE
	result =
	    con.Query("SELECT i, i=ANY(SELECT i FROM integers WHERE 1=0 AND i1.i=i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, false}));
	// subquery with OFFSET is not supported
	REQUIRE_FAIL(
	    con.Query("SELECT i, (SELECT i+i1.i FROM integers LIMIT 1 OFFSET 1) AS j FROM integers i1 ORDER BY i;"));
	// subquery with ORDER BY is not supported
	REQUIRE_FAIL(con.Query(
	    "SELECT i, (SELECT i+i1.i FROM integers ORDER BY 1 LIMIT 1 OFFSET 1) AS j FROM integers i1 ORDER BY i;"));
	// correlated filter without FROM clause
	result = con.Query("SELECT i, (SELECT 42 WHERE i1.i>2) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), 42}));
	// correlated filter with matching entry on NULL
	result = con.Query("SELECT i, (SELECT 42 WHERE i1.i IS NULL) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, Value(), Value(), Value()}));
	// scalar select with correlation in projection
	result = con.Query("SELECT i, (SELECT i+i1.i FROM integers WHERE i=1) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 4}));
	// scalar select with correlation in filter
	result = con.Query("SELECT i, (SELECT i FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	// scalar select with operation in projection
	result = con.Query("SELECT i, (SELECT i+1 FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 4}));
	// correlated scalar select with constant in projection
	result = con.Query("SELECT i, (SELECT 42 FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 42, 42, 42}));
}

TEST_CASE("Test correlated aggregate subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// aggregate with correlation in final projection
	result = con.Query("SELECT i, (SELECT MIN(i)+i1.i FROM integers) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 4}));
	// aggregate with correlation inside aggregation
	result = con.Query("SELECT i, (SELECT MIN(i+2*i1.i) FROM integers) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 5, 7}));
	result =
	    con.Query("SELECT i, SUM(i), (SELECT SUM(i)+SUM(i1.i) FROM integers) FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 7, 8, 9}));
	result = con.Query(
	    "SELECT i, SUM(i), (SELECT SUM(i)+COUNT(i1.i) FROM integers) FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {6, 7, 7, 7}));

	// correlated COUNT(*)
	result = con.Query("SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 2, 1, 0}));

	// aggregate with correlation inside aggregation
	result = con.Query("SELECT i, (SELECT MIN(i+2*i1.i) FROM integers) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 5, 7}));
	// aggregate ONLY inside subquery
	result = con.Query("SELECT (SELECT SUM(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	// aggregate ONLY inside subquery, with column reference outside of subquery
	result = con.Query("SELECT FIRST(i), (SELECT SUM(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));
	// this will fail, because "i" is not an aggregate but the SUM(i1.i) turns this query into an aggregate
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT SUM(i1.i)) FROM integers i1;"));
	REQUIRE_FAIL(con.Query("SELECT i+1, (SELECT SUM(i1.i)) FROM integers i1;"));

	result = con.Query("SELECT MIN(i), (SELECT SUM(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));

	result = con.Query("SELECT (SELECT SUM(i1.i)), (SELECT SUM(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));

	// subquery inside aggregation
	result = con.Query("SELECT SUM(i), SUM((SELECT i FROM integers WHERE i=i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));
	result = con.Query("SELECT SUM(i), (SELECT SUM(i) FROM integers WHERE i>SUM(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	// subquery with aggregation inside aggregation should fail
	REQUIRE_FAIL(con.Query("SELECT SUM((SELECT SUM(i))) FROM integers"));
	// aggregate with correlation in filter
	result = con.Query("SELECT i, (SELECT MIN(i) FROM integers WHERE i>i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, Value()}));
	// aggregate with correlation in both filter and projection
	result = con.Query("SELECT i, (SELECT MIN(i)+i1.i FROM integers WHERE i>i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 5, Value()}));
	// aggregate with correlation in GROUP BY
	result = con.Query("SELECT i, (SELECT MIN(i) FROM integers GROUP BY i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 1, 1}));
	// aggregate with correlation in HAVING clause
	result = con.Query("SELECT i, (SELECT i FROM integers GROUP BY i HAVING i=i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	// correlated subquery in HAVING
	result = con.Query("SELECT i1.i, SUM(i) FROM integers i1 GROUP BY i1.i HAVING SUM(i)=(SELECT MIN(i) FROM integers "
	                   "WHERE i<>i1.i+1) ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	result = con.Query("SELECT i % 2 AS j, SUM(i) FROM integers i1 GROUP BY j HAVING SUM(i)=(SELECT SUM(i) FROM "
	                   "integers WHERE i<>j+1) ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {4}));

	// aggregate query with non-aggregate subquery without group by
	result = con.Query("SELECT (SELECT i+SUM(i1.i) FROM integers WHERE i=1 LIMIT 1) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {7}));

	result = con.Query("SELECT (SELECT SUM(i)+SUM(i1.i) FROM integers) FROM integers i1 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {12}));
	result = con.Query("SELECT (SELECT SUM(i)+SUM((CASE WHEN i IS NOT NULL THEN i*0 ELSE 0 END)+i1.i) FROM integers) "
	                   "FROM integers i1 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 10, 14, 18}));

	// aggregate query with non-aggregate subquery with group by
	result =
	    con.Query("SELECT i, (SELECT i+SUM(i1.i) FROM integers WHERE i=1) FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 4}));

	// subquery inside aggregate
	result = con.Query("SELECT SUM((SELECT i+i1.i FROM integers WHERE i=1)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {9}));

	result =
	    con.Query("SELECT i, SUM(i1.i), (SELECT SUM(i1.i) FROM integers) AS k FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 1, 2, 3}));

	// aggregation of both entries inside subquery
	// aggregate on group inside subquery
	result =
	    con.Query("SELECT i1.i AS j, (SELECT SUM(j+i) FROM integers) AS k FROM integers i1 GROUP BY j ORDER BY j;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 9, 12, 15}));
	result = con.Query("SELECT (SELECT SUM(i1.i*i) FROM integers) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 6, 12, 18}));
	result =
	    con.Query("SELECT i, (SELECT SUM(i1.i)) AS k, (SELECT SUM(i1.i)) AS l FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 1, 2, 3}));
	// refer aggregation inside subquery
	result =
	    con.Query("SELECT i, (SELECT SUM(i1.i)*SUM(i) FROM integers) AS k FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 12, 18}));
	// refer to GROUP BY inside subquery
	result = con.Query("SELECT i AS j, (SELECT j*SUM(i) FROM integers) AS k FROM integers i1 GROUP BY j ORDER BY j;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 12, 18}));
	// refer to GROUP BY without alias but with full name
	result =
	    con.Query("SELECT i AS j, (SELECT i1.i*SUM(i) FROM integers) AS k FROM integers i1 GROUP BY j ORDER BY j;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 12, 18}));
	// perform SUM on subquery
	result =
	    con.Query("SELECT i, SUM((SELECT SUM(i)*i1.i FROM integers)) AS k FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 12, 18}));

	// aggregate subqueries cannot be nested
	REQUIRE_FAIL(con.Query(
	    "SELECT i, SUM((SELECT SUM(i)*SUM(i1.i) FROM integers)) AS k FROM integers i1 GROUP BY i ORDER BY i;"));

	// aggregation but ONLY inside subquery results in implicit aggregation
	result = con.Query("SELECT (SELECT SUM(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	result = con.Query("SELECT FIRST(i), (SELECT SUM(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));

	// aggregate that uses correlated column in aggregation
	result = con.Query("SELECT i AS j, (SELECT MIN(i1.i) FROM integers GROUP BY i HAVING i=j) FROM integers i1 GROUP "
	                   "BY j ORDER BY j;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// ORDER BY correlated subquery
	result = con.Query("SELECT i, SUM(i1.i) FROM integers i1 GROUP BY i ORDER BY (SELECT SUM(i1.i) FROM integers);");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// LIMIT 0 on correlated subquery
	result = con.Query(
	    "SELECT i, SUM((SELECT SUM(i)*i1.i FROM integers LIMIT 0)) AS k FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), Value()}));

	// GROUP BY correlated subquery
	result = con.Query(
	    "SELECT (SELECT i+i1.i FROM integers WHERE i=1) AS k, SUM(i) AS j FROM integers i1 GROUP BY k ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// correlated subquery in WHERE
	result = con.Query("SELECT SUM(i) FROM integers i1 WHERE i>(SELECT (i+i1.i)/2 FROM integers WHERE i=1);");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	// correlated aggregate in WHERE
	result = con.Query("SELECT SUM(i) FROM integers i1 WHERE i>(SELECT (SUM(i)+i1.i)/2 FROM integers WHERE i=1);");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));

	// use scalar subquery as argument to ALL/ANY
	result = con.Query("SELECT i, (SELECT MIN(i) FROM integers WHERE i=i1.i) >= ALL(SELECT i FROM integers WHERE i IS "
	                   "NOT NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, true}));
	result = con.Query("SELECT i, (SELECT MIN(i) FROM integers WHERE i<>i1.i) > ANY(SELECT i FROM integers WHERE i IS "
	                   "NOT NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, false, false}));
	result = con.Query("SELECT i, NOT((SELECT MIN(i) FROM integers WHERE i<>i1.i) > ANY(SELECT i FROM integers WHERE i "
	                   "IS NOT NULL)) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, true, true}));

	// aggregates with multiple parameters
	result = con.Query("SELECT (SELECT COVAR_POP(i1.i, i2.i) FROM integers i2) FROM integers i1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0, 0, 0}));

	result = con.Query("SELECT (SELECT COVAR_POP(i2.i, i1.i) FROM integers i2) FROM integers i1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0, 0, 0}));

	result = con.Query("SELECT (SELECT COVAR_POP(i1.i+i2.i, i1.i+i2.i) FROM integers i2) FROM integers i1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0.666667, 0.666667, 0.666667}));
}

TEST_CASE("Test correlated EXISTS subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// correlated EXISTS
	result = con.Query("SELECT i, EXISTS(SELECT i FROM integers WHERE i1.i>2) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, true}));
	result = con.Query("SELECT i, EXISTS(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, true, true, true}));
	result =
	    con.Query("SELECT i, EXISTS(SELECT i FROM integers WHERE i IS NULL OR i>i1.i*10) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {true, true, true, true}));
	result =
	    con.Query("SELECT i, EXISTS(SELECT i FROM integers WHERE i1.i>i OR i1.i IS NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {true, false, true, true}));
	result = con.Query("SELECT i FROM integers i1 WHERE EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	// correlated EXISTS with aggregations
	result = con.Query("SELECT EXISTS(SELECT i FROM integers WHERE i>MIN(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT i, SUM(i) FROM integers i1 GROUP BY i HAVING EXISTS(SELECT i FROM integers WHERE "
	                   "i>MIN(i1.i)) ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	result = con.Query("SELECT EXISTS(SELECT i+MIN(i1.i) FROM integers WHERE i=3) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT EXISTS(SELECT i+MIN(i1.i) FROM integers WHERE i=5) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	// GROUP BY correlated exists
	result = con.Query(
	    "SELECT EXISTS(SELECT i FROM integers WHERE i=i1.i) AS g, COUNT(*) FROM integers i1 GROUP BY g ORDER BY g;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 3}));
	// SUM on exists
	result = con.Query(
	    "SELECT SUM(CASE WHEN EXISTS(SELECT i FROM integers WHERE i=i1.i) THEN 1 ELSE 0 END) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// aggregates with multiple parameters
	result = con.Query("SELECT (SELECT COVAR_POP(i1.i, i2.i) FROM integers i2) FROM integers i1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0, 0, 0}));

	result = con.Query("SELECT (SELECT COVAR_POP(i2.i, i1.i) FROM integers i2) FROM integers i1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0, 0, 0}));

	result = con.Query("SELECT (SELECT COVAR_POP(i1.i+i2.i, i1.i+i2.i) FROM integers i2) FROM integers i1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0.666667, 0.666667, 0.666667}));

	result = con.Query("SELECT (SELECT COVAR_POP(i2.i, i2.i) FROM integers i2) FROM integers i1 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {0.666667, 0.666667, 0.666667, 0.666667}));

	result = con.Query("SELECT (SELECT COVAR_POP(i1.i, i1.i) FROM integers i2 LIMIT 1) FROM integers i1 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {0.666667}));
}

TEST_CASE("Test correlated ANY/ALL subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// correlated ANY/ALL
	result = con.Query("SELECT i=ANY(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, true, true}));
	result =
	    con.Query("SELECT i>ALL(SELECT (i+i1.i-1)/2 FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, true}));
	result = con.Query("SELECT i=ALL(SELECT i FROM integers WHERE i<>i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, false}));

	// correlated ANY/ALL
	result = con.Query("SELECT i FROM integers i1 WHERE i=ANY(SELECT i FROM integers WHERE i=i1.i) ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	result = con.Query("SELECT i FROM integers i1 WHERE i<>ANY(SELECT i FROM integers WHERE i=i1.i) ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT i FROM integers i1 WHERE i=ANY(SELECT i FROM integers WHERE i<>i1.i) ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT i FROM integers i1 WHERE i>ANY(SELECT i FROM integers WHERE i<>i1.i) ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	result = con.Query(
	    "SELECT i FROM integers i1 WHERE i>ALL(SELECT (i+i1.i-1)/2 FROM integers WHERE i IS NOT NULL) ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	// if there is i=ANY() where the subquery returns an EMPTY result set and i=NULL, the result becomes FALSE instead
	// of NULL
	result = con.Query("SELECT i=ALL(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true, true}));
	result = con.Query("SELECT i=ANY(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, true, true}));
	result = con.Query("SELECT i<>ALL(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, false}));
	result = con.Query("SELECT i<>ANY(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, false}));
	result = con.Query("SELECT i=ALL(SELECT i FROM integers WHERE i<>i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, false}));
	result = con.Query("SELECT i=ANY(SELECT i FROM integers WHERE i<>i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, false}));
	result = con.Query("SELECT i>ANY(SELECT i FROM integers WHERE i<>i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, true, true}));
	result = con.Query("SELECT i>ALL(SELECT (i+i1.i-1)/2 FROM integers) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, Value()}));
	result =
	    con.Query("SELECT i>ALL(SELECT (i+i1.i-1)/2 FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, true}));
	result = con.Query("SELECT i=ANY(SELECT i FROM integers WHERE i=i1.i OR i IS NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true, true, true}));
	result = con.Query("SELECT i=ALL(SELECT i FROM integers WHERE i=i1.i OR i IS NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
	// correlated ANY/ALL with aggregations
	result = con.Query("SELECT MIN(i)>ANY(SELECT i FROM integers WHERE i>MIN(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT SUM(i)>ANY(SELECT i FROM integers WHERE i>MIN(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	// correlated subquery with correlated any
	result = con.Query("SELECT (SELECT SUM(i)+SUM(i1.i) FROM integers)>ANY(SELECT i FROM integers WHERE i>MIN(i1.i)) "
	                   "FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	// zero results for all
	result = con.Query("SELECT i=ANY(SELECT i FROM integers WHERE i=i1.i AND i>10) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, false}));
}

TEST_CASE("Test for COUNT(*) and SUM(i) IS NULL in subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// COUNT(*) and SUM(i) IS NULL aggregates
	result = con.Query("SELECT i, (SELECT i FROM integers i2 WHERE i=(SELECT SUM(i) FROM integers i2 WHERE i2.i>i1.i)) "
	                   "FROM integers i1 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), 3, Value()}));
	result =
	    con.Query("SELECT i, (SELECT SUM(i) IS NULL FROM integers i2 WHERE i2.i>i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {true, false, false, true}));
	result = con.Query("SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 2, 1, 0}));
	result = con.Query(
	    "SELECT i, (SELECT COUNT(i) FROM integers i2 WHERE i2.i>i1.i OR i2.i IS NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 2, 1, 0}));
	result = con.Query(
	    "SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i OR i2.i IS NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 3, 2, 1}));
	result = con.Query("SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i OR (i1.i IS NULL AND i2.i IS "
	                   "NULL)) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 1, 0}));
	result =
	    con.Query("SELECT i FROM integers i1 WHERE (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i)=0 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 3}));
	result = con.Query("SELECT i, (SELECT i FROM integers i2 WHERE i-2=(SELECT COUNT(*) FROM integers i2 WHERE "
	                   "i2.i>i1.i)) FROM integers i1 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, Value(), 3, 2}));
	result = con.Query(
	    "SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i GROUP BY i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 1, Value()}));
	result = con.Query("SELECT i, (SELECT CASE WHEN (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i)=0 THEN 1 ELSE 0 "
	                   "END) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 0, 0, 1}));
	result = con.Query("SELECT i, (SELECT COUNT(*) FROM integers i2 WHERE i2.i>i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 2, 1, 0}));
}

TEST_CASE("Test multiple correlated columns and strings", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();

	// multiple correlated columns and strings
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER, str VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 1, 'a'), (12, 2, 'b'), (13, 3, 'c')"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (a INTEGER, c INTEGER, str2 VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (11, 1, 'a'), (12, 1, 'b'), (13, 4, 'b')"));

	result = con.Query("SELECT a, SUM(a), (SELECT SUM(a)+SUM(t1.b) FROM test) FROM test t1 GROUP BY a ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 2, {37, 38, 39}));

	// scalar query with multiple correlated columns
	result = con.Query("SELECT (SELECT test.a+test.b+SUM(test2.a) FROM test2 WHERE str=str2) FROM test ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 23, 39}));

	// exists with multiple correlated columns
	result = con.Query("SELECT * FROM test WHERE EXISTS(SELECT * FROM test2 "
	                   "WHERE test.a=test2.a AND test.b<>test2.c);");
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {"b", "c"}));

	// ANY with multiple correlated columns
	result = con.Query("SELECT a, a>=ANY(SELECT test2.a+c-b FROM test2 WHERE c>=b AND str=str2) FROM test ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {true, false, false}));

	// string comparison
	result = con.Query("SELECT str, str=ANY(SELECT str2 FROM test2) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {"a", "b", "c"}));
	REQUIRE(CHECK_COLUMN(result, 1, {true, true, false}));
	result = con.Query("SELECT str, str=ANY(SELECT str2 FROM test2 WHERE test.a<>test2.a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {"a", "b", "c"}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, true, false}));
}

TEST_CASE("Test complex correlated subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// correlated expression in subquery
	result = con.Query(
	    "SELECT i, (SELECT s1.i FROM (SELECT * FROM integers WHERE i=i1.i) s1) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	// join on two subqueries that both have a correlated expression in them
	result = con.Query("SELECT i, (SELECT s1.i FROM (SELECT i FROM integers WHERE i=i1.i) s1 INNER JOIN (SELECT i FROM "
	                   "integers WHERE i=4-i1.i) s2 ON s1.i>s2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), 3}));

	// implicit join with correlated expression in filter
	result = con.Query("SELECT i, (SELECT s1.i FROM integers s1, integers s2 WHERE s1.i=s2.i AND s1.i=4-i1.i) AS j "
	                   "FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 2, 1}));
	// join with a correlated expression in the join condition
	result = con.Query("SELECT i, (SELECT s1.i FROM integers s1 INNER JOIN integers s2 ON s1.i=s2.i AND s1.i=4-i1.i) "
	                   "AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 2, 1}));
	// inner join on correlated subquery
	result = con.Query("SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT 2*SUM(i)*s1.i FROM "
	                   "integers)=(SELECT SUM(i)*s2.i FROM integers) ORDER BY s1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	// inner join on non-equality subquery
	result = con.Query("SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT s1.i=s2.i) ORDER BY s1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	result = con.Query("SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT s1.i=i FROM integers WHERE s2.i=i) "
	                   "ORDER BY s1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	// left outer join on correlated subquery
	result = con.Query("SELECT * FROM integers s1 LEFT OUTER JOIN integers s2 ON (SELECT 2*SUM(i)*s1.i FROM "
	                   "integers)=(SELECT SUM(i)*s2.i FROM integers) ORDER BY s1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, Value(), Value()}));

	// left outer join in correlated expression
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT SUM(s1.i) FROM integers s1 LEFT OUTER JOIN integers s2 ON s1.i=s2.i OR "
	                       "s1.i=i1.i-1) AS j FROM integers i1 ORDER BY i;"));
	// REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	// REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 9, 12}));
	// full outer join: both sqlite and postgres actually cannot run this one
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT SUM(s1.i) FROM integers s1 FULL OUTER JOIN integers s2 ON s1.i=s2.i OR "
	                       "s1.i=i1.i-1) AS j FROM integers i1 ORDER BY i;"));
	// REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	// REQUIRE(CHECK_COLUMN(result, 1, {6, 6, 9, 12}));

	// correlated expression inside window function not supported
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT row_number() OVER (ORDER BY i)) FROM integers i1 ORDER BY i;"));

	// union with correlated expression
	result = con.Query("SELECT i, (SELECT i FROM integers WHERE i=i1.i UNION SELECT i FROM integers WHERE i=i1.i) AS j "
	                   "FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	// except with correlated expression
	result = con.Query("SELECT i, (SELECT i FROM integers WHERE i IS NOT NULL EXCEPT SELECT i FROM integers WHERE "
	                   "i<>i1.i) AS j FROM integers i1 WHERE i IS NOT NULL ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	// intersect with correlated expression
	result = con.Query("SELECT i, (SELECT i FROM integers WHERE i=i1.i INTERSECT SELECT i FROM integers WHERE i=i1.i) "
	                   "AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	// multiple setops
	result = con.Query("SELECT i, (SELECT i FROM integers WHERE i=i1.i UNION SELECT i FROM integers WHERE i<>i1.i "
	                   "EXCEPT SELECT i FROM integers WHERE i<>i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// uncorrelated query inside correlated query
	result = con.Query("SELECT i, (SELECT (SELECT SUM(i) FROM integers)+42+i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 49, 50, 51}));
}

TEST_CASE("Test window functions in correlated subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// window functions in correlated subquery
	result = con.Query(
	    "SELECT i, (SELECT row_number() OVER (ORDER BY i) FROM integers WHERE i1.i=i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 1, 1}));
	result = con.Query("SELECT i1.i, (SELECT rank() OVER (ORDER BY i) FROM integers WHERE i1.i=i) FROM integers i1, "
	                   "integers i2 ORDER BY i1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value(), 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), Value(), 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
	result = con.Query("SELECT i1.i, (SELECT row_number() OVER (ORDER BY i) FROM integers WHERE i1.i=i) FROM integers "
	                   "i1, integers i2 ORDER BY i1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value(), 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), Value(), 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
	result = con.Query(
	    "SELECT i, (SELECT SUM(i) OVER (ORDER BY i) FROM integers WHERE i1.i=i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	result = con.Query("SELECT i, (SELECT SUM(s1.i) OVER (ORDER BY s1.i) FROM integers s1, integers s2 WHERE i1.i=s1.i "
	                   "LIMIT 1) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 4, 8, 12}));
}

TEST_CASE("Test nested correlated subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// nested correlated queries
	result = con.Query("SELECT i, (SELECT (SELECT 42+i1.i)+42+i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 86, 88, 90}));
	result = con.Query("SELECT i, (SELECT (SELECT (SELECT (SELECT 42+i1.i)++i1.i)+42+i1.i)+42+i1.i) AS j FROM integers "
	                   "i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 130, 134, 138}));
	result = con.Query("SELECT i, (SELECT (SELECT i1.i+SUM(i2.i)) FROM integers i2) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 7, 8, 9}));
	// correlated query inside uncorrelated query
	result = con.Query(
	    "SELECT i, (SELECT (SELECT (SELECT (SELECT i1.i+i1.i+i1.i+i1.i+i1.i)))) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 5, 10, 15}));
	result = con.Query("SELECT i, (SELECT SUM(i)+(SELECT 42+i1.i) FROM integers) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 49, 50, 51}));
	result = con.Query(
	    "SELECT i, (SELECT ((SELECT ((SELECT ((SELECT SUM(i)+SUM(i4.i)+SUM(i3.i)+SUM(i2.i)+SUM(i1.i) FROM integers "
	    "i5)) FROM integers i4)) FROM integers i3)) FROM integers i2) AS j FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 25, 26, 27}));
	result = con.Query("SELECT i, (SELECT (SELECT (SELECT (SELECT i1.i+i1.i+i1.i+i1.i+i1.i+i2.i) FROM integers i2 "
	                   "WHERE i2.i=i1.i))) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 12, 18}));
	result = con.Query("SELECT (SELECT (SELECT SUM(i1.i)+SUM(i2.i)+SUM(i3.i) FROM integers i3) FROM integers i2) FROM "
	                   "integers i1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {18}));

	// explicit join on subquery
	result = con.Query("SELECT i, (SELECT SUM(s1.i) FROM integers s1 INNER JOIN integers s2 ON (SELECT "
	                   "i1.i+s1.i)=(SELECT i1.i+s2.i)) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 6, 6}));
	// nested aggregate queries
	result = con.Query("SELECT i, SUM(i), (SELECT (SELECT SUM(i)+SUM(i1.i)+SUM(i2.i) FROM integers) FROM integers i2) "
	                   "FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 13, 14, 15}));

	// correlated ANY inside subquery
	result = con.Query("SELECT i, (SELECT SUM(ss1.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM "
	                   "integers WHERE i<>s1.i)) ss1) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 5, 5, 5}));
	result = con.Query("SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i "
	                   "FROM integers WHERE i=s1.i)) ss2) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// left outer join on correlated subquery within subquery
	// not supported yet: left outer join on JoinSide::BOTH
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT SUM(s1.i) FROM integers s1 LEFT OUTER JOIN integers s2 ON (SELECT "
	                       "i1.i+s1.i)=(SELECT i1.i+s2.i)) AS j FROM integers i1 ORDER BY i;"));
	// REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	// REQUIRE(CHECK_COLUMN(result, 1, {6, 6, 6, 6}));
	result =
	    con.Query("SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM "
	              "integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i=ANY(SELECT i FROM "
	              "integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 10, 10, 10}));
	// left outer join with correlation on LHS
	result = con.Query("SELECT i, (SELECT SUM(s1.i) FROM (SELECT i FROM integers WHERE i=i1.i) s1 LEFT OUTER JOIN "
	                   "integers s2 ON s1.i=s2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	result = con.Query("SELECT i, (SELECT SUM(s1.i) FROM (SELECT i FROM integers WHERE i<>i1.i) s1 LEFT OUTER JOIN "
	                   "integers s2 ON s1.i=s2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 5, 4, 3}));
	// left outer join with correlation on RHS
	result = con.Query("SELECT i, (SELECT SUM(s2.i) FROM integers s1 LEFT OUTER JOIN (SELECT i FROM integers WHERE "
	                   "i=i1.i) s2 ON s1.i=s2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	result = con.Query("SELECT i, (SELECT SUM(s2.i) FROM integers s1 LEFT OUTER JOIN (SELECT i FROM integers WHERE "
	                   "i<>i1.i) s2 ON s1.i=s2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 5, 4, 3}));

	result = con.Query(
	    "SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE CASE WHEN (i=i1.i AND i=ANY(SELECT i FROM "
	    "integers WHERE i=s1.i)) THEN true ELSE false END) ss2) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	result = con.Query("SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i "
	                   "FROM integers WHERE i=s1.i)) ss2) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	result = con.Query("SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i) ss2) AS j FROM "
	                   "integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	result = con.Query("SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=ANY(SELECT i FROM "
	                   "integers WHERE i=s1.i)) ss2) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {6, 6, 6, 6}));
	result = con.Query("SELECT i, (SELECT i=ANY(SELECT i FROM integers WHERE i=s1.i) FROM integers s1 WHERE i=i1.i) AS "
	                   "j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, true}));
	result = con.Query("SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i OR i=ANY(SELECT i "
	                   "FROM integers WHERE i=s1.i)) ss2) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {6, 6, 6, 6}));
	result = con.Query(
	    "SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE CASE WHEN (i=i1.i AND i=ANY(SELECT i FROM "
	    "integers WHERE i=s1.i)) THEN true ELSE false END) ss2) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	result = con.Query("SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND EXISTS(SELECT i "
	                   "FROM integers WHERE i=s1.i)) ss2) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// complex left outer join with correlation on RHS
	result = con.Query("SELECT i, (SELECT SUM(ss1.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM "
	                   "integers WHERE i<>s1.i)) ss1) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 5, 5, 5}));
	result =
	    con.Query("SELECT i, (SELECT SUM(ss1.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers "
	              "WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM "
	              "integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 5, 5, 5}));
	result =
	    con.Query("SELECT i, (SELECT SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM integers "
	              "WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i=i1.i AND i=ANY(SELECT i FROM "
	              "integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), 2, 3}));
	result =
	    con.Query("SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i>ANY(SELECT i FROM "
	              "integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i=i1.i AND "
	              "i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), 7, 8}));
	// complex left outer join with correlation on LHS
	result =
	    con.Query("SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND "
	              "i>ANY(SELECT i FROM integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE "
	              "i=ANY(SELECT i FROM integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), 4, 6}));
	// complex left outer join with correlation on both sides
	result = con.Query(
	    "SELECT i, (SELECT SUM(ss1.i)+SUM(ss2.i) FROM (SELECT i FROM integers s1 WHERE i=i1.i AND i>ANY(SELECT i FROM "
	    "integers WHERE i<>s1.i)) ss1 LEFT OUTER JOIN (SELECT i FROM integers s1 WHERE i<>i1.i OR i=ANY(SELECT i FROM "
	    "integers WHERE i=s1.i)) ss2 ON ss1.i=ss2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), 4, 6}));
	// test correlated queries with correlated expressions inside FROM clause
	// subquery
	result = con.Query("SELECT i, (SELECT * FROM (SELECT (SELECT 42+i1.i)) s1) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 43, 44, 45}));
	// cross product
	result = con.Query("SELECT i, (SELECT s1.k+s2.k FROM (SELECT (SELECT 42+i1.i) AS k) s1, (SELECT (SELECT 42+i1.i) "
	                   "AS k) s2) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 86, 88, 90}));
	// join
	result = con.Query("SELECT i, (SELECT s1.k+s2.k FROM (SELECT (SELECT 42+i1.i) AS k) s1 LEFT OUTER JOIN (SELECT "
	                   "(SELECT 42+i1.i) AS k) s2 ON s1.k=s2.k) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 86, 88, 90}));

	// IN list inside correlated subquery
	result = con.Query("SELECT i, (SELECT i1.i IN (1, 2, 3, 4, 5, 6, 7, 8)) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, true}));

	// nested correlated subqueries with multiple aggregate parameters
	result = con.Query("SELECT (SELECT (SELECT COVAR_POP(i1.i, i3.i) FROM integers i3) FROM integers i2 LIMIT 1) FROM "
	                   "integers i1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0, 0, 0}));

	result = con.Query("SELECT (SELECT (SELECT COVAR_POP(i2.i, i3.i) FROM integers i3) FROM integers i2 LIMIT 1) FROM "
	                   "integers i1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, 0}));
}

TEST_CASE("Test varchar correlated subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();
	// varchar tests
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello'), ('world'), (NULL)"));
	// ANY
	result = con.Query("SELECT NULL IN (SELECT * FROM strings WHERE v=s1.v) FROM strings s1 ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {false, Value(), Value()}));
	result = con.Query("SELECT 3 IN (SELECT * FROM strings WHERE v=s1.v) FROM strings s1 ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false}));
	result = con.Query("SELECT 'hello' IN (SELECT * FROM strings WHERE v=s1.v) FROM strings s1 ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, false}));
	result = con.Query("SELECT 'bla' IN (SELECT * FROM strings WHERE v=s1.v) FROM strings s1 ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false}));
	result =
	    con.Query("SELECT 'hello' IN (SELECT * FROM strings WHERE v=s1.v or v IS NULL) FROM strings s1 ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true, Value()}));
	result = con.Query("SELECT 'bla' IN (SELECT * FROM strings WHERE v=s1.v or v IS NULL) FROM strings s1 ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	// EXISTS
	result = con.Query("SELECT * FROM strings WHERE EXISTS(SELECT NULL, v) ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello", "world"}));
	result =
	    con.Query("SELECT * FROM strings s1 WHERE EXISTS(SELECT v FROM strings WHERE v=s1.v OR v IS NULL) ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello", "world"}));
	result = con.Query("SELECT * FROM strings s1 WHERE EXISTS(SELECT v FROM strings WHERE v=s1.v) ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));
	// // scalar query
	result = con.Query("SELECT (SELECT v FROM strings WHERE v=s1.v) FROM strings s1 ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello", "world"}));
	result = con.Query(
	    "SELECT (SELECT v FROM strings WHERE v=s1.v OR (v='hello' AND s1.v IS NULL)) FROM strings s1 ORDER BY v");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "hello", "world"}));
}

TEST_CASE("Test correlated subqueries based on TPC-DS", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE item(i_manufact INTEGER)"));

	REQUIRE_NO_FAIL(con.Query(
	    "SELECT * FROM item i1 WHERE (SELECT count(*) AS item_cnt FROM item WHERE (i_manufact = i1.i_manufact AND "
	    "i_manufact=3) OR (i_manufact = i1.i_manufact AND i_manufact=3)) > 0 ORDER BY 1 LIMIT 100;"));
	REQUIRE_NO_FAIL(con.Query(
	    "SELECT * FROM item i1 WHERE (SELECT count(*) AS item_cnt FROM item WHERE (i_manufact = i1.i_manufact AND "
	    "i_manufact=3) OR (i_manufact = i1.i_manufact AND i_manufact=3)) ORDER BY 1 LIMIT 100;"));
}

TEST_CASE("Test correlated subquery with grouping columns", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  "
	                          "varchar(64), TotalSales int); "));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO "
	                          "Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), "
	                          "(111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);"));

	result = con.Query("SELECT col1 IN (SELECT ColID FROM tbl_ProductSales) FROM another_T;");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, false}));
	result = con.Query("SELECT col1 IN (SELECT ColID + col1 FROM tbl_ProductSales) FROM another_T;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, false}));
	result = con.Query("SELECT col1 IN (SELECT ColID + col1 FROM tbl_ProductSales) FROM another_T GROUP BY col1;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, false}));
	result =
	    con.Query("SELECT col1 IN (SELECT ColID + another_T.col1 FROM tbl_ProductSales) FROM another_T GROUP BY col1;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, false}));
	result = con.Query(
	    "SELECT (col1 + 1) AS k, k IN (SELECT ColID + k FROM tbl_ProductSales) FROM another_T GROUP BY k ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 12, 112, 1112}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, false}));
	result = con.Query(
	    "SELECT (col1 + 1) IN (SELECT ColID + (col1 + 1) FROM tbl_ProductSales) FROM another_T GROUP BY (col1 + 1);");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, false}));

	// this should fail, col1 + 42 is not a grouping column
	REQUIRE_FAIL(con.Query("SELECT col1+1, col1+42 FROM another_T GROUP BY col1+1;"));
	// this should also fail, col1 + 42 is not a grouping column
	REQUIRE_FAIL(con.Query(
	    "SELECT (col1 + 1) IN (SELECT ColID + (col1 + 42) FROM tbl_ProductSales) FROM another_T GROUP BY (col1 + 1);"));

	// having without GROUP BY in subquery
	result = con.Query("SELECT col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MIN(col8) IS NULL) FROM another_T "
	                   "GROUP BY col1, col2, col5, col8;");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true, true}));
	result = con.Query("SELECT CASE WHEN 1 IN (SELECT MAX(col7) UNION ALL (SELECT MIN(ColID) FROM tbl_ProductSales "
	                   "INNER JOIN another_T t2 ON t2.col5 = t2.col1)) THEN 2 ELSE NULL END FROM another_T t1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT CASE WHEN 1 IN (SELECT (SELECT MAX(col7))) THEN 2 ELSE NULL END FROM another_T t1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	// UNION ALL with correlated subquery on either side
	result =
	    con.Query("SELECT CASE WHEN 1 IN (SELECT (SELECT MAX(col7)) UNION ALL (SELECT MIN(ColID) FROM tbl_ProductSales "
	              "INNER JOIN another_T t2 ON t2.col5 = t2.col1)) THEN 2 ELSE NULL END FROM another_T t1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT CASE WHEN 1 IN (SELECT (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 "
	                   "ON t2.col5 = t2.col1) UNION ALL (SELECT MAX(col7))) THEN 2 ELSE NULL END FROM another_T t1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// correlated column comparison with correlated subquery
	result = con.Query("SELECT (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t1.col7 <> (SELECT "
	                   "MAX(t1.col1 + t3.col4) FROM another_T t3)) FROM another_T t1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1}));
	result = con.Query("SELECT (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t1.col7 <> "
	                   "ANY(SELECT MAX(t1.col1 + t3.col4) FROM another_T t3)) FROM another_T t1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1}));

	// LEFT JOIN between correlated columns not supported for now
	REQUIRE_FAIL(con.Query(
	    "SELECT CASE WHEN NOT col1 NOT IN (SELECT (SELECT MAX(col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales "
	    "LEFT JOIN another_T t2 ON t2.col5 = t1.col1)) THEN 1 ELSE 2 END FROM another_T t1 GROUP BY col1 ORDER BY 1;"));
	// REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 2, 2}));

	// correlated columns in window functions not supported yet
	REQUIRE_FAIL(con.Query("SELECT EXISTS (SELECT RANK() OVER (PARTITION BY SUM(DISTINCT col5))) FROM another_T t1;"));
	// REQUIRE(CHECK_COLUMN(result, 0, {true}));
	REQUIRE_FAIL(con.Query("SELECT (SELECT SUM(col2) OVER (PARTITION BY SUM(col2) ORDER BY MAX(col1 + ColID) ROWS "
	                       "UNBOUNDED PRECEDING) FROM tbl_ProductSales) FROM another_T t1 GROUP BY col1"));
}
