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
	// cout << con.GetProfilingInformation();
	return;
	// push condition down
	result = con.Query("SELECT * FROM integers i1, integers i2 WHERE i1.i=(SELECT i FROM integers WHERE i1.i=i) AND "
	                   "i1.i=i2.i ORDER BY i1.i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	cout << con.GetProfilingInformation();
	// test filter pushdown into subquery
	result =
	    con.Query("SELECT * FROM (SELECT i1.i AS a, i2.i AS b FROM integers i1, integers i2) a1 WHERE a=b ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	cout << con.GetProfilingInformation();
	// filter pushdown on subquery with more complicated expression
	result = con.Query(
	    "SELECT * FROM (SELECT i1.i=i2.i AS cond FROM integers i1, integers i2) a1 WHERE NOT cond ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, false, false, false}));
	cout << con.GetProfilingInformation();
	// filter pushdown into union in subquery
	result = con.Query("SELECT * FROM (SELECT * FROM integers i1 UNION SELECT * FROM integers i2) a WHERE i=3;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	cout << con.GetProfilingInformation();
	// filter pushdown on subquery with window function (cannot be done because it will mess up the ordering)
	result = con.Query("SELECT * FROM (SELECT i1.i AS a, i2.i AS b, row_number() OVER (ORDER BY i1.i, i2.i) FROM "
	                   "integers i1, integers i2 WHERE i1.i IS NOT NULL AND i2.i IS NOT NULL) a1 WHERE a=b ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 5, 9}));
}
