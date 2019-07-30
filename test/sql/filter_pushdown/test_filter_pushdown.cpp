#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

#include <iostream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test filter pushdown", "[filterpushdown]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// test filter pushdown into cross product
	// single filter that matches both sides
	result = con.Query("SELECT * FROM integers i1, integers i2 WHERE i1.i=i2.i ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	// add filter that matches left side
	result = con.Query("SELECT * FROM integers i1, integers i2 WHERE i1.i=i2.i AND i1.i>1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));
	// three cross products
	result = con.Query(
	    "SELECT * FROM integers i1, integers i2, integers i3 WHERE i1.i=i2.i AND i1.i=i3.i AND i1.i>1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {2, 3}));
	// inner join
	result = con.Query("SELECT * FROM integers i1 JOIN integers i2 ON i1.i=i2.i WHERE i1.i>1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));
	// left outer join
	// condition on LHS
	result = con.Query("SELECT * FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=1 WHERE i1.i>2 ORDER BY 2");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	// condition on RHS that eliminates NULL values
	result =
	    con.Query("SELECT * FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=0 WHERE i2.i IS NOT NULL ORDER BY 2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	// more complicated conditions on RHS that eliminates NULL values
	result = con.Query("SELECT * FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=0 WHERE i2.i>1 ORDER BY 2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	result = con.Query("SELECT * FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=0 WHERE CASE WHEN i2.i IS NULL THEN "
	                   "False ELSE True END ORDER BY 2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	// conditions on RHS that does not eliminate NULL values
	result = con.Query(
	    "SELECT DISTINCT * FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=0 WHERE i2.i IS NULL ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), Value()}));
	// conditions on both sides that guarantees to eliminate null values from RHS
	result = con.Query("SELECT * FROM integers i1 LEFT OUTER JOIN integers i2 ON 1=1 WHERE i1.i=i2.i ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	// MARK join
	// transform into semi join
	result = con.Query("SELECT * FROM integers WHERE i IN ((SELECT * FROM integers)) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	// transform into ANTI join
	result = con.Query("SELECT * FROM integers WHERE i NOT IN ((SELECT * FROM integers WHERE i=1)) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	// condition pushdown
	result = con.Query("SELECT * FROM integers WHERE i IN ((SELECT * FROM integers)) AND i<3 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	result = con.Query(
	    "SELECT * FROM integers i1, integers i2 WHERE i1.i IN ((SELECT * FROM integers)) AND i1.i=i2.i ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	// DELIM join
	// correlated exists: turn into semi join
	result = con.Query("SELECT * FROM integers i1 WHERE EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i1.i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	// correlated not exists: turn into anti join
	result = con.Query("SELECT * FROM integers i1 WHERE NOT EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i1.i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	// push condition down delim join
	result = con.Query("SELECT * FROM integers i1, integers i2 WHERE i1.i=(SELECT i FROM integers WHERE i1.i=i) AND "
	                   "i1.i=i2.i ORDER BY i1.i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	// test filter pushdown into subquery
	result =
	    con.Query("SELECT * FROM (SELECT i1.i AS a, i2.i AS b FROM integers i1, integers i2) a1 WHERE a=b ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	// filter pushdown on subquery with more complicated expression
	result =
	    con.Query("SELECT * FROM (SELECT i1.i=i2.i AS cond FROM integers i1, integers i2) a1 WHERE cond ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true}));

	// filter pushdown into distinct in subquery
	result = con.Query(
	    "SELECT * FROM (SELECT DISTINCT i1.i AS a, i2.i AS b FROM integers i1, integers i2) res WHERE a=1 AND b=3;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	// filter pushdown into union in subquery
	result = con.Query("SELECT * FROM (SELECT * FROM integers i1 UNION SELECT * FROM integers i2) a WHERE i=3;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	// filter pushdown on subquery with window function (cannot be done because it will mess up the ordering)
	result = con.Query("SELECT * FROM (SELECT i1.i AS a, i2.i AS b, row_number() OVER (ORDER BY i1.i, i2.i) FROM "
	                   "integers i1, integers i2 WHERE i1.i IS NOT NULL AND i2.i IS NOT NULL) a1 WHERE a=b ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 5, 9}));
	// condition on scalar projection
	result = con.Query("SELECT * FROM (SELECT 0=1 AS cond FROM integers i1, integers i2) a1 WHERE cond ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// condition on scalar grouping
	result = con.Query(
	    "SELECT * FROM (SELECT 0=1 AS cond FROM integers i1, integers i2 GROUP BY 1) a1 WHERE cond ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
}

TEST_CASE("Test filter pushdown with more data", "[filterpushdown][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableProfiling();

	// in this test we run queries that will take a long time without filter pushdown, but are almost instant with
	// proper filter pushdown we create two tables with 10K elements each in most tests we cross product them together
	// in some way to create a "big table" (100M entries) but the filter can be pushed past the cross product in all
	// cases

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vals1(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("PREPARE s1 AS INSERT INTO vals1 VALUES ($1, $2);"));
	for (size_t i = 0; i < 10000; i++) {
		REQUIRE_NO_FAIL(con.Query("EXECUTE s1(" + to_string(i) + ", " + to_string(i) + ");"));
	}
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vals2(k INTEGER, l INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO vals2 SELECT * FROM vals1"));
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));

	// pushdown filters into subqueries
	result = con.Query("SELECT i, k FROM (SELECT i, k FROM vals1, vals2) tbl1 WHERE i=k AND i<5 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2, 3, 4}));
	// pushdown past DISTINCT
	result = con.Query("SELECT i, k FROM (SELECT DISTINCT i, k FROM vals1, vals2) tbl1 WHERE i=k AND i<5 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2, 3, 4}));
	// pushdown conditions on group variables
	result = con.Query("SELECT i, k, SUM(j) FROM vals1, vals2 GROUP BY i, k HAVING i=k AND i<5 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 1, 2, 3, 4}));
	// also inside subqueries
	result = con.Query(
	    "SELECT i, k, SUM(j) FROM (SELECT * FROM vals1, vals2) tbl1 GROUP BY i, k HAVING i=k AND i<5 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 1, 2, 3, 4}));
	// and also like this
	result = con.Query("SELECT i, k, sum FROM (SELECT i, k, SUM(j) AS sum FROM vals1, vals2 GROUP BY i, k) tbl1 WHERE "
	                   "i=k AND i<5 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 1, 2, 3, 4}));

	// LEFT OUTER JOIN on constant "true" can be turned into cross product, and after filters can be pushed
	result = con.Query("SELECT * FROM vals1 LEFT OUTER JOIN vals2 ON 1=1 WHERE i=k AND k=5");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
	// left outer join with equality filter can be turned into INNER JOIN
	result = con.Query("SELECT * FROM vals1 LEFT OUTER JOIN vals2 ON 1=1 WHERE i=k ORDER BY i LIMIT 5");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 3, {0, 1, 2, 3, 4}));
	// left outer join can be turned into inner join after which elements can be pushed down into RHS
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 WHERE j=5 AND l=5) tbl1 LEFT OUTER JOIN (SELECT * "
	                   "FROM vals1, vals2) tbl2 ON tbl1.i=tbl2.i AND tbl1.k=tbl2.k WHERE tbl2.j=5 AND tbl2.l=5;");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
	REQUIRE(CHECK_COLUMN(result, 4, {5}));
	REQUIRE(CHECK_COLUMN(result, 5, {5}));
	REQUIRE(CHECK_COLUMN(result, 6, {5}));
	REQUIRE(CHECK_COLUMN(result, 7, {5}));
	// filters can be pushed in the LHS of the LEFT OUTER JOIN
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2) tbl1 LEFT OUTER JOIN (SELECT * FROM vals1, vals2 "
	                   "WHERE i=5 AND k=10) tbl2 ON tbl1.i=tbl2.i AND tbl1.k=tbl2.k WHERE tbl1.i=5 AND tbl1.k=10");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5}));
	REQUIRE(CHECK_COLUMN(result, 2, {10}));
	REQUIRE(CHECK_COLUMN(result, 3, {10}));
	REQUIRE(CHECK_COLUMN(result, 4, {5}));
	REQUIRE(CHECK_COLUMN(result, 5, {5}));
	REQUIRE(CHECK_COLUMN(result, 6, {10}));
	REQUIRE(CHECK_COLUMN(result, 7, {10}));

	// conditions in the ON clause can be pushed down into the RHS
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 WHERE i=5 AND k=5) tbl1 LEFT OUTER JOIN (SELECT * "
	                   "FROM vals1, vals2) tbl2 ON tbl2.i=5 AND tbl2.k=5");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
	REQUIRE(CHECK_COLUMN(result, 4, {5}));
	REQUIRE(CHECK_COLUMN(result, 5, {5}));
	REQUIRE(CHECK_COLUMN(result, 6, {5}));
	REQUIRE(CHECK_COLUMN(result, 7, {5}));
	// also works if condition filters everything
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 WHERE i=5 AND k=5) tbl1 LEFT OUTER JOIN (SELECT * "
	                   "FROM vals1, vals2) tbl2 ON tbl2.i>10000 AND tbl2.k=5");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 6, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 7, {Value()}));
	// we can replicate conditions on the left join predicates on the RHS
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2) tbl1 LEFT OUTER JOIN (SELECT * FROM vals1, vals2) "
	                   "tbl2 ON tbl1.i=tbl2.i AND tbl1.k=tbl2.k WHERE tbl1.i=5 AND tbl1.k=10");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5}));
	REQUIRE(CHECK_COLUMN(result, 2, {10}));
	REQUIRE(CHECK_COLUMN(result, 3, {10}));
	REQUIRE(CHECK_COLUMN(result, 4, {5}));
	REQUIRE(CHECK_COLUMN(result, 5, {5}));
	REQUIRE(CHECK_COLUMN(result, 6, {10}));
	REQUIRE(CHECK_COLUMN(result, 7, {10}));
	// also multiple conditions
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2) tbl1 LEFT OUTER JOIN (SELECT * FROM vals1, vals2) "
	                   "tbl2 ON tbl1.i=tbl2.i AND tbl1.k=tbl2.k WHERE tbl1.i>4 AND tbl1.i<6 AND tbl1.k=10");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {5}));
	REQUIRE(CHECK_COLUMN(result, 2, {10}));
	REQUIRE(CHECK_COLUMN(result, 3, {10}));
	REQUIRE(CHECK_COLUMN(result, 4, {5}));
	REQUIRE(CHECK_COLUMN(result, 5, {5}));
	REQUIRE(CHECK_COLUMN(result, 6, {10}));
	REQUIRE(CHECK_COLUMN(result, 7, {10}));
	// pushdown union
	result =
	    con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 UNION SELECT * FROM vals1, vals2) tbl1 WHERE i=3 AND k=5");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
	// pushdown into except
	result = con.Query(
	    "SELECT * FROM (SELECT * FROM vals1, vals2 EXCEPT SELECT * FROM vals1, vals2) tbl1 WHERE i=3 AND k=5");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	REQUIRE(CHECK_COLUMN(result, 2, {}));
	REQUIRE(CHECK_COLUMN(result, 3, {}));
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 EXCEPT SELECT * FROM vals1, vals2 WHERE i<>1) tbl1 "
	                   "WHERE i<5 AND k<5 ORDER BY 1, 2, 3, 4;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 3, {0, 1, 2, 3, 4}));
	// pushdown intersect
	result = con.Query(
	    "SELECT * FROM (SELECT * FROM vals1, vals2 INTERSECT SELECT * FROM vals1, vals2) tbl1 WHERE i=3 AND k=5");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
	// constant condition on scalar projection
	result = con.Query("SELECT * FROM (SELECT 0=1 AS cond FROM vals1, vals2) a1 WHERE cond ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// constant condition that is more hidden
	result = con.Query("SELECT * FROM (SELECT 1 AS a FROM vals1, vals2) a1 WHERE a=0 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// condition on scalar grouping
	result = con.Query("SELECT * FROM (SELECT 0=1 AS cond FROM vals1, vals2 GROUP BY 1) a1 WHERE cond ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM (SELECT 1 AS a FROM vals1, vals2 GROUP BY a) a1 WHERE a=0 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	// duplicate filters across equivalency sets and pushdown cross product
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i=tbl1.k AND tbl1.i=tbl2.k AND tbl1.i=tbl2.i AND tbl1.i=5000;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// also push other comparisons
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i=tbl1.k AND tbl1.i=tbl2.k AND tbl1.i=tbl2.i AND tbl1.i>4999 AND tbl1.i<5001;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// empty result
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i=5000 AND tbl1.i<>5000;");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	// also if we have a transitive condition
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i=5000 AND tbl1.i=tbl2.i AND tbl2.i<>5000;");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	// useless inequality checks should be pruned
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i=5000 AND tbl1.i=tbl2.i AND tbl1.i=tbl2.k AND tbl1.i=tbl1.k AND tbl2.i<>5001;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// add many useless predicates
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl2.i>10 AND tbl1.k>=500 AND tbl2.k<7000 AND tbl2.k<=6000 AND tbl2.k<>8000 AND "
	                   "tbl1.i<>4000 AND tbl1.i=tbl2.i AND tbl1.i=tbl2.k AND tbl1.i=tbl1.k AND tbl1.i=5000;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// // filter equivalence with expressions
	// result = con.Query("SELECT COUNT(*) FROM vals1, vals2 WHERE i+1=5001 AND j=l AND k=i AND l+1=5001");
	// REQUIRE(CHECK_COLUMN(result, 0, {0}));
	// result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2 WHERE i+1=5000 AND k+1=5000) tbl1, (SELECT *
	// FROM vals1, vals2) tbl2 WHERE tbl1.i=tbl2.i AND tbl1.k=tbl2.k;"); REQUIRE(CHECK_COLUMN(result, 0, {0}));
	return;

	// greater than/less than should also be transitive
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i>9997 AND tbl1.k>tbl1.i AND tbl2.i>tbl1.i AND tbl2.k>tbl1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	cout << con.GetProfilingInformation();
	// equality with constant and then GT
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i=9998 AND tbl1.k=9998 AND tbl2.i>tbl1.i AND tbl2.k>tbl1.k;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	cout << con.GetProfilingInformation();
	// equality with constant and then LT
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i=1 AND tbl1.k=1 AND tbl2.i<tbl1.i AND tbl2.k<tbl1.k;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	cout << con.GetProfilingInformation();
	// transitive GT/LT
	result = con.Query("SELECT COUNT(*) FROM vals1, vals2 WHERE i>4999 AND j<=l AND k>=i AND l<5001");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	cout << con.GetProfilingInformation();

	// these more advanced cases we don't support yet
	// filter equivalence with expressions
	// SELECT COUNT(*) FROM vals1 v1,
	// vals1 v2 WHERE v1.i+v2.i=10; IN list result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1,
	// (SELECT * FROM vals1, vals2) tbl2 WHERE tbl2.k IN (5000, 5001, 5002) AND tbl2.k<5000;");
	// REQUIRE(CHECK_COLUMN(result, 0, {0}));
	// // CASE expression
	// result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2
	// WHERE tbl2.k<5000 AND CASE WHEN (tbl2.k>5000) THEN (tbl2.k=5001) ELSE (tbl2.k=5000) END;");
	// REQUIRE(CHECK_COLUMN(result, 0, {0}));
	// // OR expression
	// result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2
	// WHERE tbl2.k<5000 AND (tbl2.k=5000 OR tbl2.k>5000);"); REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Test filter pushdown with more advanced expressions", "[filterpushdown][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableProfiling();

	// in this test we run queries that will take a long time without filter pushdown, but are almost instant with
	// proper filter pushdown we create two tables with 10K elements each in most tests we cross product them together
	// in some way to create a "big table" (100M entries) but the filter can be pushed past the cross product in all
	// cases
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vals1(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("PREPARE s1 AS INSERT INTO vals1 VALUES ($1, $2);"));
	for (size_t i = 0; i < 10000; i++) {
		REQUIRE_NO_FAIL(con.Query("EXECUTE s1(" + to_string(i) + ", " + to_string(i) + ");"));
	}
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vals2(k INTEGER, l INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO vals2 SELECT * FROM vals1"));
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));

	// x + 1 = 5001
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i+1=5001 AND tbl1.i<>5000;");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	// x - 1 = 4999
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i-1=4999 AND tbl1.i<>5000;");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	// x * 2 = 10000
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i*2=10000 AND tbl1.i<>5000;");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	// // x * 2 = 9999 should always return false (as 9999 % 2 != 0, it's not cleanly divisible)
	// result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2
	// WHERE tbl1.i*2=9999;"); REQUIRE(CHECK_COLUMN(result, 0, {0})); x / 2 = 2500 result = con.Query("SELECT COUNT(*)
	// FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 WHERE tbl1.i/2=2500 AND
	// tbl1.i<>5000;"); REQUIRE(CHECK_COLUMN(result, 0, {0})); -x=-5000
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE -tbl1.i=-5000 AND tbl1.i<>5000;");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	return;
	// x + (1 + 1) = 5002
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2) tbl1, (SELECT * FROM vals1, vals2) tbl2 "
	                   "WHERE tbl1.i+(1+1)=5002 AND tbl1.i<>5000;");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Test moving/duplicating conditions", "[filterpushdown][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableProfiling();

	// in this test we run queries that will take a long time without filter pushdown, but are almost instant with
	// proper filter pushdown we create two tables with 10K elements each in most tests we cross product them together
	// in some way to create a "big table" (100M entries) but the filter can be pushed past the cross product in all
	// cases
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vals1(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("PREPARE s1 AS INSERT INTO vals1 VALUES ($1, $2);"));
	for (size_t i = 0; i < 10000; i++) {
		REQUIRE_NO_FAIL(con.Query("EXECUTE s1(" + to_string(i) + ", " + to_string(i) + ");"));
	}
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vals2(k INTEGER, l INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO vals2 SELECT * FROM vals1"));
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));

	return;

	// move conditions between joins
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 WHERE i=3 AND k=5) tbl1 INNER JOIN (SELECT * FROM "
	                   "vals1, vals2) tbl2 ON tbl1.i=tbl2.i AND tbl1.k=tbl2.k;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
	REQUIRE(CHECK_COLUMN(result, 4, {3}));
	REQUIRE(CHECK_COLUMN(result, 5, {3}));
	REQUIRE(CHECK_COLUMN(result, 6, {5}));
	REQUIRE(CHECK_COLUMN(result, 7, {5}));
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 WHERE i>5000) tbl1 INNER JOIN (SELECT * FROM vals1, "
	                   "vals2 WHERE i<5000) tbl2 ON tbl1.i=tbl2.i AND tbl1.k=tbl2.k;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	REQUIRE(CHECK_COLUMN(result, 2, {}));
	REQUIRE(CHECK_COLUMN(result, 3, {}));
	REQUIRE(CHECK_COLUMN(result, 4, {}));
	REQUIRE(CHECK_COLUMN(result, 5, {}));
	REQUIRE(CHECK_COLUMN(result, 6, {}));
	REQUIRE(CHECK_COLUMN(result, 7, {}));
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 WHERE i>5000) tbl1 INNER JOIN (SELECT * FROM vals1, "
	                   "vals2 WHERE i<5002 AND k=1) tbl2 ON tbl1.i=tbl2.i AND tbl1.k=tbl2.k;");
	REQUIRE(CHECK_COLUMN(result, 0, {5001}));
	REQUIRE(CHECK_COLUMN(result, 1, {5001}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));
	REQUIRE(CHECK_COLUMN(result, 4, {5001}));
	REQUIRE(CHECK_COLUMN(result, 5, {5001}));
	REQUIRE(CHECK_COLUMN(result, 6, {1}));
	REQUIRE(CHECK_COLUMN(result, 7, {1}));
	// left outer join conditions
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 WHERE i>5000) tbl1 LEFT OUTER JOIN (SELECT * FROM "
	                   "vals1, vals2) tbl2 ON tbl1.i=tbl2.i AND tbl1.k=tbl2.k WHERE tbl1.i<5002 AND tbl1.k=1;");
	REQUIRE(CHECK_COLUMN(result, 0, {5001}));
	REQUIRE(CHECK_COLUMN(result, 1, {5001}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));
	REQUIRE(CHECK_COLUMN(result, 4, {5001}));
	REQUIRE(CHECK_COLUMN(result, 5, {5001}));
	REQUIRE(CHECK_COLUMN(result, 6, {1}));
	REQUIRE(CHECK_COLUMN(result, 7, {1}));
	// only RHS has conditions
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2) tbl1 LEFT OUTER JOIN (SELECT * FROM vals1, vals2 "
	                   "WHERE i=3 AND k=5) tbl2 ON tbl1.i=tbl2.i AND tbl1.k=tbl2.k WHERE tbl2.i<5000;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
	REQUIRE(CHECK_COLUMN(result, 4, {3}));
	REQUIRE(CHECK_COLUMN(result, 5, {3}));
	REQUIRE(CHECK_COLUMN(result, 6, {5}));
	REQUIRE(CHECK_COLUMN(result, 7, {5}));
	// only RHS has conditions
	result = con.Query(
	    "SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM vals1, vals2) tbl1 LEFT OUTER JOIN (SELECT * FROM vals1, "
	    "vals2 WHERE i=3 AND k=5) tbl2 ON tbl1.i=tbl2.i WHERE tbl1.k<10 AND tbl2.k IS NOT NULL) tbl3;");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));
	// only LHS has conditions
	result = con.Query("SELECT COUNT(*) FROM (SELECT * FROM vals1, vals2 WHERE i=3 AND k=5) tbl1 LEFT OUTER JOIN "
	                   "(SELECT * FROM vals1, vals2) tbl2 ON tbl1.i=tbl2.i AND tbl1.k=tbl2.k;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// side channel EXCEPT/INTERSECT
	result =
	    con.Query("SELECT * FROM vals1, vals2 WHERE i>5000 INTERSECT SELECT * FROM vals1, vals2 WHERE i<5002 AND k=1;");
	REQUIRE(CHECK_COLUMN(result, 0, {5001}));
	REQUIRE(CHECK_COLUMN(result, 1, {5001}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));
	result = con.Query("SELECT * FROM vals1, vals2 WHERE i>5000 AND i<5002 AND k=1 EXCEPT SELECT * FROM vals1, vals2;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	REQUIRE(CHECK_COLUMN(result, 2, {}));
	REQUIRE(CHECK_COLUMN(result, 3, {}));
	// side channel GROUP conditions
	result = con.Query("SELECT * FROM (SELECT i, k, MIN(j) FROM vals1, vals2 WHERE i=1 AND k=3 GROUP BY i, k) tbl1 "
	                   "INNER JOIN (SELECT * FROM vals1, vals2) tbl2 ON tbl1.i=tbl2.i AND tbl1.k=tbl2.k;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));
	REQUIRE(CHECK_COLUMN(result, 4, {1}));
	REQUIRE(CHECK_COLUMN(result, 5, {1}));
	REQUIRE(CHECK_COLUMN(result, 6, {3}));
	REQUIRE(CHECK_COLUMN(result, 7, {3}));
	// conditions in subqueries
	// uncorrelated subqueries
	result = con.Query("SELECT * FROM vals1 WHERE i IN (SELECT i FROM vals1, vals2) AND i=3;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	result = con.Query("SELECT * FROM vals1 WHERE EXISTS(SELECT i FROM vals1, vals2) AND i=3;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	// correlated subqueries
	result =
	    con.Query("SELECT * FROM vals1 v1 WHERE i IN (SELECT i FROM vals1, vals2 WHERE i=v1.i AND k=v1.i) AND i=3;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	result = con.Query(
	    "SELECT * FROM vals1 v1 WHERE i IN (SELECT i FROM vals1, vals2 WHERE i=v1.i AND k=v1.i AND k=4) AND i=3;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	result = con.Query("SELECT * FROM vals1 v1 WHERE i IN (SELECT i FROM vals1, vals2 WHERE i=v1.i AND k=v1.i AND "
	                   "k>5000) AND i<5002;");
	REQUIRE(CHECK_COLUMN(result, 0, {5001}));
	REQUIRE(CHECK_COLUMN(result, 1, {5001}));
	result = con.Query("SELECT * FROM vals1 v1 WHERE i=(SELECT i FROM vals1, vals2 WHERE i=v1.i AND k=v1.i) AND i=3;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	result =
	    con.Query("SELECT * FROM vals1 v1 WHERE i=(SELECT MIN(i) FROM vals1, vals2 WHERE i=v1.i AND k=v1.i) AND i=3;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
}
