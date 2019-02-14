#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test simple uncorrelated subqueries", "[subquery]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// scalar subqueries
	result = con.Query("SELECT * FROM integers WHERE i=(SELECT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT * FROM integers WHERE i=(SELECT SUM(1))");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT * FROM integers WHERE i=(SELECT MIN(i) FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT * FROM integers WHERE i=(SELECT MAX(i) FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT *, (SELECT MAX(i) FROM integers) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3, 3, 3}));
	// group by on subquery
	result = con.Query("SELECT (SELECT 42) AS k, MAX(i) FROM integers GROUP BY k");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	// subquery as parameter to aggregate
	result = con.Query("SELECT i, MAX((SELECT 42)) FROM integers GROUP BY i ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, 42, 42, 42}));

	// scalar subquery returning zero results should result in NULL
	result = con.Query("SELECT (SELECT * FROM integers WHERE i>10) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));

	// return more than one row in a scalar subquery
	// controversial: in postgres this gives an error
	// but SQLite accepts it and just uses the first value
	// we choose to agree with SQLite here
	result = con.Query("SELECT * FROM integers WHERE i=(SELECT i FROM integers WHERE i IS NOT NULL ORDER BY i)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// i.e. the above query is equivalent to this query
	result = con.Query("SELECT * FROM integers WHERE i=(SELECT i FROM integers WHERE i IS NOT NULL ORDER BY i LIMIT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// returning multiple columns should fail though
	REQUIRE_FAIL(con.Query("SELECT * FROM integers WHERE i=(SELECT 1, 2)"));
	REQUIRE_FAIL(con.Query("SELECT * FROM integers WHERE i=(SELECT i, i + 2 FROM integers)"));
	// but not for EXISTS queries!
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM integers WHERE EXISTS (SELECT 1, 2)"));
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM integers WHERE EXISTS (SELECT i, i + 2 FROM integers)"));

	//  uncorrelated subquery in SELECT
	result = con.Query("SELECT (SELECT i FROM integers WHERE i=1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT * FROM integers WHERE i > (SELECT i FROM integers WHERE i=1)");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));

	// uncorrelated EXISTS
	result = con.Query("SELECT * FROM integers WHERE EXISTS(SELECT 1) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	result = con.Query("SELECT * FROM integers WHERE EXISTS(SELECT * FROM integers) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	result = con.Query("SELECT * FROM integers WHERE NOT EXISTS(SELECT * FROM integers) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM integers WHERE EXISTS(SELECT NULL) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));

	// exists in SELECT clause
	result = con.Query("SELECT EXISTS(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT EXISTS(SELECT * FROM integers WHERE i>10)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	// multiple exists
	result = con.Query("SELECT EXISTS(SELECT * FROM integers), EXISTS(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	REQUIRE(CHECK_COLUMN(result, 1, {true}));

	// exists used in operations
	result = con.Query("SELECT EXISTS(SELECT * FROM integers) AND EXISTS(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	// nested EXISTS
	result = con.Query("SELECT EXISTS(SELECT EXISTS(SELECT * FROM integers))");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	// uncorrelated IN
	result = con.Query("SELECT * FROM integers WHERE 1 IN (SELECT 1) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	result = con.Query("SELECT * FROM integers WHERE 1 IN (SELECT * FROM integers) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	result = con.Query("SELECT * FROM integers WHERE 1 IN (SELECT NULL::INTEGER) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	// scalar NULL results
	result = con.Query("SELECT 1 IN (SELECT NULL::INTEGER) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
	result = con.Query("SELECT NULL IN (SELECT * FROM integers) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value()}));
	
	// add aggregations after the subquery
	result = con.Query("SELECT SUM(i) FROM integers WHERE 1 IN (SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));

	// uncorrelated ANY
	result = con.Query("SELECT i FROM integers WHERE i <= ANY(SELECT i FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	result = con.Query("SELECT i FROM integers WHERE i > ANY(SELECT i FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	result = con.Query("SELECT i, i > ANY(SELECT i FROM integers) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), true, true}));
	result = con.Query("SELECT i, i > ANY(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, true, true}));
	result = con.Query("SELECT i, NULL > ANY(SELECT i FROM integers) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), Value()}));
	result = con.Query("SELECT i, NULL > ANY(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), Value()}));
	result = con.Query("SELECT i FROM integers WHERE i = ANY(SELECT i FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	result = con.Query("SELECT i, i = ANY(SELECT i FROM integers WHERE i>2) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, true}));
	result = con.Query("SELECT i, i = ANY(SELECT i FROM integers WHERE i>2 OR i IS NULL) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), true}));
	result = con.Query("SELECT i, i <> ANY(SELECT i FROM integers WHERE i>2) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, false}));
	result = con.Query("SELECT i, i <> ANY(SELECT i FROM integers WHERE i>2 OR i IS NULL) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, Value()}));
	// use a bunch of cross products to make bigger data sets (> STANDARD_VECTOR_SIZE)
	result = con.Query("SELECT i, i = ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers i5, integers i6 WHERE i1.i IS NOT NULL) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, true}));
	result = con.Query("SELECT i, i = ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers i5, integers i6 WHERE i1.i IS NOT NULL AND i1.i <> 2) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, false, true}));
	result = con.Query("SELECT i, i >= ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers i5, integers i6 WHERE i1.i IS NOT NULL) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, true}));
	result = con.Query("SELECT i, i >= ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers i5, integers i6 WHERE i1.i IS NOT NULL AND i1.i <> 1 LIMIT 1) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, true, true}));

	// uncorrelated ALL
	result = con.Query("SELECT i FROM integers WHERE i >= ALL(SELECT i FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT i, i >= ALL(SELECT i FROM integers) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, Value()}));
	result = con.Query("SELECT i FROM integers WHERE i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT i, i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, true}));
	
	result = con.Query("SELECT i FROM integers WHERE i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT i FROM integers WHERE i > ALL(SELECT MIN(i) FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	result = con.Query("SELECT i FROM integers WHERE i < ALL(SELECT MAX(i) FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	result = con.Query("SELECT i FROM integers WHERE i <= ALL(SELECT i FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT i FROM integers WHERE i <= ALL(SELECT i FROM integers WHERE i IS NOT NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT i FROM integers WHERE i = ALL(SELECT i FROM integers WHERE i=1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT i FROM integers WHERE i <> ALL(SELECT i FROM integers WHERE i=1)");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	result = con.Query("SELECT i FROM integers WHERE i = ALL(SELECT i FROM integers WHERE i IS NOT NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT i FROM integers WHERE i <> ALL(SELECT i FROM integers WHERE i IS NOT NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	// nested uncorrelated subqueries
	result = con.Query("SELECT (SELECT (SELECT (SELECT 42)))");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	result = con.Query("SELECT (SELECT EXISTS(SELECT * FROM integers WHERE i>2)) FROM integers;");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true, true}));

	result = con.Query("SELECT (SELECT MAX(i) FROM integers) AS k, SUM(i) FROM integers GROUP BY k;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));

	// subqueries in GROUP BY clause
	result = con.Query("SELECT i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL) AS k, SUM(i) FROM integers GROUP BY k ORDER BY k;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 3}));

	result = con.Query("SELECT SUM(i) FROM integers GROUP BY (i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL)) ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 3, 3}));

	result = con.Query("SELECT i >= ALL(SELECT MIN(i) FROM integers WHERE i IS NOT NULL) AS k, SUM(i) FROM integers GROUP BY k ORDER BY k;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6}));

	// subquery in CASE statement
	result = con.Query("SELECT i, SUM(CASE WHEN (i >= ALL(SELECT i FROM integers WHERE i=2)) THEN 1 ELSE 0 END) FROM integers GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 0, 1, 1}));

	// subquery in HAVING
	result = con.Query("SELECT i % 2 AS k, SUM(i) FROM integers GROUP BY k HAVING SUM(i) > (SELECT MAX(i) FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {4}));

	result = con.Query("SELECT i FROM integers WHERE NOT(i IN (SELECT i FROM integers WHERE i>1));");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test simple correlated subqueries", "[subquery]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// con.EnableQueryVerification();
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
	// subquery with OFFSET is not supported
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT 42+i1.i FROM integers LIMIT 1 OFFSET 1) AS j FROM integers i1 ORDER BY i;"));
	// subquery with ORDER BY is not supported
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT 42+i1.i FROM integers ORDER BY 1 LIMIT 1 OFFSET 1) AS j FROM integers i1 ORDER BY i;"));
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
	// aggregate with correlation in final projection
	result = con.Query("SELECT i, (SELECT MIN(i)+i1.i FROM integers) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 4}));
	//aggregate with correlation inside aggregation
	result = con.Query("SELECT i, (SELECT MIN(i+2*i1.i) FROM integers) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 5, 7}));
	result = con.Query("SELECT i, SUM(i), (SELECT SUM(i)+SUM(i1.i) FROM integers) FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 7, 8, 9}));
	result = con.Query("SELECT i, SUM(i), (SELECT SUM(i)+COUNT(i1.i) FROM integers) FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {6, 7, 7, 7}));

	//aggregate with correlation inside aggregation
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

	// subquery inside aggregation
	result = con.Query("SELECT SUM(i), SUM((SELECT i FROM integers WHERE i=i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));
	result = con.Query("SELECT SUM(i), (SELECT SUM(i) FROM integers WHERE i>SUM(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
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
	result = con.Query("SELECT i % 2 AS j, SUM(i) FROM integers i1 GROUP BY j HAVING SUM(i)=(SELECT SUM(i) FROM integers WHERE i<>j+1) ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {4}));

	// aggregate query with non-aggregate subquery without group by
	result = con.Query("SELECT (SELECT i+SUM(i1.i) FROM integers WHERE i=1 LIMIT 1) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {7}));

	// aggregate query with non-aggregate subquery with group by
	result = con.Query("SELECT i, (SELECT i+SUM(i1.i) FROM integers WHERE i=1) FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 4}));

	// subquery inside aggregate
	result = con.Query("SELECT SUM((SELECT i+i1.i FROM integers WHERE i=1)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {9}));

	result = con.Query("SELECT i, SUM(i1.i), (SELECT SUM(i1.i) FROM integers) AS k FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 1, 2, 3}));

	// aggregation of both entries inside subquery
	// aggregate on group inside subquery
	result = con.Query("SELECT i1.i AS j, (SELECT SUM(j+i) FROM integers) AS k FROM integers i1 GROUP BY j ORDER BY j;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 9, 12, 15}));
	result = con.Query("SELECT (SELECT SUM(i1.i*i) FROM integers) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 6, 12, 18}));
	result = con.Query("SELECT i, (SELECT SUM(i1.i)) AS k, (SELECT SUM(i1.i)) AS l FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 1, 2, 3}));
	// refer aggregation inside subquery
	result = con.Query("SELECT i, (SELECT SUM(i1.i)*SUM(i) FROM integers) AS k FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 12, 18}));
	// refer to GROUP BY inside subquery
	result = con.Query("SELECT i AS j, (SELECT j*SUM(i) FROM integers) AS k FROM integers i1 GROUP BY j ORDER BY j;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 12, 18}));
	// perform SUM on subquery
	result = con.Query("SELECT i, SUM((SELECT SUM(i)*i1.i FROM integers)) AS k FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 12, 18}));

	// aggregate subqueries cannot be nested
	REQUIRE_FAIL(con.Query("SELECT i, SUM((SELECT SUM(i)*SUM(i1.i) FROM integers)) AS k FROM integers i1 GROUP BY i ORDER BY i;"));
	
	// aggregation but ONLY inside subquery results in implicit aggregation
	result = con.Query("SELECT (SELECT SUM(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	result = con.Query("SELECT FIRST(i), (SELECT SUM(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));
	
	// aggregate that uses correlated column in aggregation
	result = con.Query("SELECT i AS j, (SELECT MIN(i1.i) FROM integers GROUP BY i HAVING i=j) FROM integers i1 GROUP BY j ORDER BY j;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// ORDER BY correlated subquery
	result = con.Query("SELECT i, SUM(i1.i) FROM integers i1 GROUP BY i ORDER BY (SELECT SUM(i1.i) FROM integers);");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// GROUP BY correlated subquery
	result = con.Query("SELECT (SELECT i+i1.i FROM integers WHERE i=1) AS k, SUM(i) AS j FROM integers i1 GROUP BY k ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// correlated subquery in WHERE
	result = con.Query("SELECT SUM(i) FROM integers i1 WHERE i>(SELECT (i+i1.i)/2 FROM integers WHERE i=1);");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	// correlated aggregate in WHERE
	result = con.Query("SELECT SUM(i) FROM integers i1 WHERE i>(SELECT (SUM(i)+i1.i)/2 FROM integers WHERE i=1);");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));

	// correlated columns in window functions not supported
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT row_number() OVER (ORDER BY i)) FROM integers;"));
	// window function in correlated subquery not supported either
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT row_number() OVER (ORDER BY i) FROM integers WHERE i>i1.i LIMIT 1) FROM integers i1 ORDER BY i;"));

	return;
	// join with a correlated expression in the join condition
	result = con.Query("SELECT i, (SELECT s1.i FROM integers s1 INNER JOIN integers s2 ON s1.i=s2.i AND s1.i=4-i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 2, 1}));
	// join on two subqueries that both have a correlated expression in them
	result = con.Query("SELECT i, (SELECT s1.i FROM (SELECT i FROM integers WHERE i=i1.i) s1 INNER JOIN (SELECT i FROM integers WHERE i=4-i1.i) s2 ON s1.i>s2.i LIMIT 1) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), 3}));
	// union with correlated expression
	result = con.Query("SELECT i, (SELECT i FROM integers UNION SELECT i FROM integers WHERE i=i1.i ORDER BY 1 LIMIT 1) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), Value()}));
	// except with correlated expression
	result = con.Query("SELECT i, (SELECT i FROM integers WHERE i IS NOT NULL EXCEPT SELECT i FROM integers WHERE i<>i1.i) AS j FROM integers i1 WHERE i IS NOT NULL ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));

	// use scalar subquery as argument to ALL/ANY
	result = con.Query("SELECT i, (SELECT MIN(i) FROM integers WHERE i=i1.i) >= ALL(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, true}));
	result = con.Query("SELECT i, (SELECT MIN(i) FROM integers WHERE i<>i1.i) > ANY(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true, false, false}));
	result = con.Query("SELECT i, NOT((SELECT MIN(i) FROM integers WHERE i<>i1.i) > ANY(SELECT i FROM integers WHERE i IS NOT NULL)) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, true, true}));

	// nested aggregate queries
	result = con.Query("SELECT i, SUM(i), (SELECT (SELECT SUM(i)+SUM(i1.i)+SUM(i2.i) FROM integers) FROM integers i2) FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 13, 14, 15}));
	// nested correlated queries
	result = con.Query("SELECT i, (SELECT (SELECT 42+i1.i)+42+i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 86, 88, 90}));
}

TEST_CASE("Test subqueries from the paper 'Unnesting Arbitrary Subqueries'", "[subquery]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE students(id INTEGER, name VARCHAR, major VARCHAR, year INTEGER)"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE exams(sid INTEGER, course VARCHAR, curriculum VARCHAR, grade INTEGER, year INTEGER)"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO students VALUES (1, 'Mark', 'CS', 2017)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO students VALUES (2, 'Dirk', 'CS', 2017)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO exams VALUES (1, 'Database Systems', 'CS', 10, 2015)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO exams VALUES (1, 'Graphics', 'CS', 9, 2016)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO exams VALUES (2, 'Database Systems', 'CS', 7, 2015)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO exams VALUES (2, 'Graphics', 'CS', 7, 2016)"));

	result = con.Query("SELECT s.name, e.course, e.grade FROM students s, exams e WHERE s.id=e.sid AND e.grade=(SELECT "
	                   "MAX(e2.grade) FROM exams e2 WHERE s.id=e2.sid) ORDER BY name, course;");
	REQUIRE(CHECK_COLUMN(result, 0, {"Dirk", "Dirk", "Mark"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Database Systems", "Graphics", "Database Systems"}));
	REQUIRE(CHECK_COLUMN(result, 2, {7, 7, 10}));

	result = con.Query("SELECT s.name, e.course, e.grade FROM students s, exams e WHERE s.id=e.sid AND (s.major = 'CS' "
	                   "OR s.major = 'Games Eng') AND e.grade <= (SELECT AVG(e2.grade) - 1 FROM exams e2 WHERE "
	                   "s.id=e2.sid OR (e2.curriculum=s.major AND s.year>=e2.year)) ORDER BY name, course;");
	REQUIRE(CHECK_COLUMN(result, 0, {"Dirk", "Dirk"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Database Systems", "Graphics"}));
	REQUIRE(CHECK_COLUMN(result, 2, {7, 7}));

	result = con.Query("SELECT name, major FROM students s WHERE EXISTS(SELECT * FROM exams e WHERE e.sid=s.id AND "
	                   "grade=10) OR s.name='Dirk' ORDER BY name");
	REQUIRE(CHECK_COLUMN(result, 0, {"Dirk", "Mark"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"CS", "CS"}));
}
