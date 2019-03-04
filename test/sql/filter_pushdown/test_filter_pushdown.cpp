#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test filter pushdown", "[filterpushdown]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
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
	result = con.Query("SELECT * FROM (SELECT i1.i=i2.i AS cond FROM integers i1, integers i2) a1 WHERE cond ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true}));
	
	// filter pushdown into distinct in subquery
	result = con.Query("SELECT * FROM (SELECT DISTINCT i1.i AS a, i2.i AS b FROM integers i1, integers i2) res WHERE a=1 AND b=3;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	// filter pushdown into union in subquery
	result = con.Query("SELECT * FROM (SELECT * FROM integers i1 UNION SELECT * FROM integers i2) a WHERE i=3;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	// filter pushdown on subquery with window function (cannot be done because it will mess up the ordering)
	result = con.Query("SELECT * FROM (SELECT i1.i AS a, i2.i AS b, row_number() OVER (ORDER BY i1.i, i2.i) FROM "
	                   "integers i1, integers i2 WHERE i1.i IS NOT NULL AND i2.i IS NOT NULL) a1 WHERE a=b ORDER BY 1");
	cout << con.GetProfilingInformation();
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 5, 9}));
}

TEST_CASE("Test filter pushdown with more data", "[filterpushdown][.]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.EnableQueryVerification();
	con.EnableProfiling();

	// in this test we run queries that will take a long time without filter pushdown, but are almost instant with proper filter pushdown
	// we create two tables with 10K elements each
	// in most tests we cross product them together in some way to create a "big table" (100M entries)
	// but the filter can be pushed past the cross product in all cases

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vals1(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("PREPARE s1 AS INSERT INTO vals1 VALUES ($1, $2);"));
	for(size_t i = 0; i < 10000; i++) {
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
	result = con.Query("SELECT i, k, SUM(j) FROM (SELECT * FROM vals1, vals2) tbl1 GROUP BY i, k HAVING i=k AND i<5 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 1, 2, 3, 4}));
	// pushdown filters in LEFT OUTER JOIN
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
	// pushdown union
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 UNION SELECT * FROM vals1, vals2) tbl1 WHERE i=3 AND k=5");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
	// pushdown into except
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 EXCEPT SELECT * FROM vals1, vals2) tbl1 WHERE i=3 AND k=5");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	REQUIRE(CHECK_COLUMN(result, 2, {}));
	REQUIRE(CHECK_COLUMN(result, 3, {}));
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 EXCEPT SELECT * FROM vals1, vals2 WHERE i<>1) tbl1 WHERE i<5 AND k<5 ORDER BY 1, 2, 3, 4;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 3, {0, 1, 2, 3, 4}));
	// pushdown intersect
	result = con.Query("SELECT * FROM (SELECT * FROM vals1, vals2 INTERSECT SELECT * FROM vals1, vals2) tbl1 WHERE i=3 AND k=5");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {5}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
}