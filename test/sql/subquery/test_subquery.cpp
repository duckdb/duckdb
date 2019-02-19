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

	con.EnableProfiling();
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
	// zero results always results in TRUE for ALL, even if "i" is NULL
	result = con.Query("SELECT i FROM integers WHERE i <> ALL(SELECT i FROM integers WHERE i>10) ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	result = con.Query("SELECT i, i <> ALL(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {true, true, true, true}));
	// zero results always results in FALSE for ANY
	result = con.Query("SELECT i, i > ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, false}));
	result = con.Query("SELECT i, i = ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, false}));
	result = con.Query("SELECT i, i >= ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, false}));
	result = con.Query("SELECT i, i <= ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, false}));
	result = con.Query("SELECT i, i < ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, false}));
	result = con.Query("SELECT i, i <> ANY(SELECT i FROM integers WHERE i>10) FROM integers ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, false}));

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

	// multiple subqueries in select without FROM
	result = con.Query("SELECT (SELECT SUM(i) FROM integers), (SELECT 42)");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {42}));
}

TEST_CASE("Test simple correlated subqueries", "[subquery]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));
	
	// nested correlated queries
	// result = con.Query("SELECT i, (SELECT (SELECT 42+i1.i)+42+i1.i) AS j FROM integers i1 ORDER BY i;");
	// REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	// REQUIRE(CHECK_COLUMN(result, 1, {Value(), 86, 88, 90}));
	
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
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT i+i1.i FROM integers LIMIT 1 OFFSET 1) AS j FROM integers i1 ORDER BY i;"));
	// subquery with ORDER BY is not supported
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT i+i1.i FROM integers ORDER BY 1 LIMIT 1 OFFSET 1) AS j FROM integers i1 ORDER BY i;"));
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
	result = con.Query("SELECT i1.i, SUM(i) FROM integers i1 GROUP BY i1.i HAVING SUM(i)=(SELECT MIN(i) FROM integers WHERE i<>i1.i+1) ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	result = con.Query("SELECT i % 2 AS j, SUM(i) FROM integers i1 GROUP BY j HAVING SUM(i)=(SELECT SUM(i) FROM integers WHERE i<>j+1) ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {4}));

	// aggregate query with non-aggregate subquery without group by
	result = con.Query("SELECT (SELECT i+SUM(i1.i) FROM integers WHERE i=1 LIMIT 1) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {7}));

	result = con.Query("SELECT (SELECT SUM(i)+SUM(i1.i) FROM integers) FROM integers i1 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {12}));
	result = con.Query("SELECT (SELECT SUM(i)+SUM((CASE WHEN i IS NOT NULL THEN i*0 ELSE 0 END)+i1.i) FROM integers) FROM integers i1 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 10, 14, 18}));

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

	// use scalar subquery as argument to ALL/ANY
	result = con.Query("SELECT i, (SELECT MIN(i) FROM integers WHERE i=i1.i) >= ALL(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, true}));
	result = con.Query("SELECT i, (SELECT MIN(i) FROM integers WHERE i<>i1.i) > ANY(SELECT i FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, false, false}));
	result = con.Query("SELECT i, NOT((SELECT MIN(i) FROM integers WHERE i<>i1.i) > ANY(SELECT i FROM integers WHERE i IS NOT NULL)) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, true, true}));

	// correlated EXISTS
	result = con.Query("SELECT i, EXISTS(SELECT i FROM integers WHERE i1.i>2) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, false, false, true}));
	result = con.Query("SELECT i, EXISTS(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {false, true, true, true}));
	result = con.Query("SELECT i FROM integers i1 WHERE EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	// correlated EXISTS with aggregations
	result = con.Query("SELECT EXISTS(SELECT i FROM integers WHERE i>MIN(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT i, SUM(i) FROM integers i1 GROUP BY i HAVING EXISTS(SELECT i FROM integers WHERE i>MIN(i1.i)) ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	result = con.Query("SELECT EXISTS(SELECT i+MIN(i1.i) FROM integers WHERE i=3) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT EXISTS(SELECT i+MIN(i1.i) FROM integers WHERE i=5) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	// GROUP BY correlated exists
	result = con.Query("SELECT EXISTS(SELECT i FROM integers WHERE i=i1.i) AS g, COUNT(*) FROM integers i1 GROUP BY g ORDER BY g;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 3}));
	// SUM on exists
	result = con.Query("SELECT SUM(CASE WHEN EXISTS(SELECT i FROM integers WHERE i=i1.i) THEN 1 ELSE 0 END) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// correlated ANY/ALL
	result = con.Query("SELECT i=ANY(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, true, true}));
	result = con.Query("SELECT i>ALL(SELECT (i+i1.i-1)/2 FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;");
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
	result = con.Query("SELECT i FROM integers i1 WHERE i>ALL(SELECT (i+i1.i-1)/2 FROM integers WHERE i IS NOT NULL) ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	// if there is i=ANY() where the subquery returns an EMPTY result set and i=NULL, the result becomes FALSE instead of NULL
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
	result = con.Query("SELECT i>ALL(SELECT (i+i1.i-1)/2 FROM integers WHERE i IS NOT NULL) FROM integers i1 ORDER BY i;");
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
	result = con.Query("SELECT (SELECT SUM(i)+SUM(i1.i) FROM integers)>ANY(SELECT i FROM integers WHERE i>MIN(i1.i)) FROM integers i1;");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	// zero results for all
	result = con.Query("SELECT i=ANY(SELECT i FROM integers WHERE i=i1.i AND i>10) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false, false}));
	
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

	// correlated expression in subquery
	result = con.Query("SELECT i, (SELECT s1.i FROM (SELECT * FROM integers WHERE i=i1.i) s1) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	// join on two subqueries that both have a correlated expression in them
	result = con.Query("SELECT i, (SELECT s1.i FROM (SELECT i FROM integers WHERE i=i1.i) s1 INNER JOIN (SELECT i FROM integers WHERE i=4-i1.i) s2 ON s1.i>s2.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), 3}));

	// implicit join with correlated expression in filter
	result = con.Query("SELECT i, (SELECT s1.i FROM integers s1, integers s2 WHERE s1.i=s2.i AND s1.i=4-i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 2, 1}));
	// join with a correlated expression in the join condition
	result = con.Query("SELECT i, (SELECT s1.i FROM integers s1 INNER JOIN integers s2 ON s1.i=s2.i AND s1.i=4-i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 2, 1}));
	// inner join on correlated subquery
	result = con.Query("SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT 2*SUM(i)*s1.i FROM integers)=(SELECT SUM(i)*s2.i FROM integers) ORDER BY s1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));
	// inner join on non-equality subquery
	result = con.Query("SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT s1.i=s2.i) ORDER BY s1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	result = con.Query("SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT s1.i=i FROM integers WHERE s2.i=i) ORDER BY s1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	// left outer join on correlated subquery
	result = con.Query("SELECT * FROM integers s1 LEFT OUTER JOIN integers s2 ON (SELECT 2*SUM(i)*s1.i FROM integers)=(SELECT SUM(i)*s2.i FROM integers) ORDER BY s1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, Value(), Value()}));

	// left outer join in correlated expression
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT SUM(s1.i) FROM integers s1 LEFT OUTER JOIN integers s2 ON s1.i=s2.i OR s1.i=i1.i-1) AS j FROM integers i1 ORDER BY i;"));
	// REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	// REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 9, 12}));
	// full outer join: both sqlite and postgres actually cannot run this one
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT SUM(s1.i) FROM integers s1 FULL OUTER JOIN integers s2 ON s1.i=s2.i OR s1.i=i1.i-1) AS j FROM integers i1 ORDER BY i;"));
	// REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	// REQUIRE(CHECK_COLUMN(result, 1, {6, 6, 9, 12}));
	
	// window functions in correlated subquery
	result = con.Query("SELECT i, (SELECT row_number() OVER (ORDER BY i) FROM integers WHERE i1.i=i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 1, 1}));
	result = con.Query("SELECT i1.i, (SELECT rank() OVER (ORDER BY i) FROM integers WHERE i1.i=i) FROM integers i1, integers i2 ORDER BY i1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value(), 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), Value(), 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
	result = con.Query("SELECT i1.i, (SELECT row_number() OVER (ORDER BY i) FROM integers WHERE i1.i=i) FROM integers i1, integers i2 ORDER BY i1.i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value(), Value(), 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), Value(), 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
	result = con.Query("SELECT i, (SELECT SUM(i) OVER (ORDER BY i) FROM integers WHERE i1.i=i) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	result = con.Query("SELECT i, (SELECT SUM(s1.i) OVER (ORDER BY s1.i) FROM integers s1, integers s2 WHERE i1.i=s1.i LIMIT 1) FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 4, 8, 12}));

	// correlated expression inside window function not supported
	REQUIRE_FAIL(con.Query("SELECT i, (SELECT row_number() OVER (ORDER BY i)) FROM integers i1 ORDER BY i;"));

	// union with correlated expression
	result = con.Query("SELECT i, (SELECT i FROM integers WHERE i=i1.i UNION SELECT i FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	// except with correlated expression
	result = con.Query("SELECT i, (SELECT i FROM integers WHERE i IS NOT NULL EXCEPT SELECT i FROM integers WHERE i<>i1.i) AS j FROM integers i1 WHERE i IS NOT NULL ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
	// intersect with correlated expression
	result = con.Query("SELECT i, (SELECT i FROM integers WHERE i=i1.i INTERSECT SELECT i FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	// multiple setops
	result = con.Query("SELECT i, (SELECT i FROM integers WHERE i=i1.i UNION SELECT i FROM integers WHERE i<>i1.i EXCEPT SELECT i FROM integers WHERE i<>i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// nested correlated queries
	// result = con.Query("SELECT i, (SELECT (SELECT 42+i1.i)+42+i1.i) AS j FROM integers i1 ORDER BY i;");
	// REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	// REQUIRE(CHECK_COLUMN(result, 1, {Value(), 86, 88, 90}));


	return;

	// nested aggregate queries
	result = con.Query("SELECT i, SUM(i), (SELECT (SELECT SUM(i)+SUM(i1.i)+SUM(i2.i) FROM integers) FROM integers i2  ORDER BY 1 LIMIT 1) FROM integers i1 GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 13, 14, 15}));
	// nested correlated queries
	result = con.Query("SELECT i, (SELECT (SELECT 42+i1.i)+42+i1.i) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 86, 88, 90}));
	// left outer join on subquery within subquery
	result = con.Query("SELECT i, (SELECT SUM(s1.i) FROM integers s1 LEFT OUTER JOIN integers s2 ON (SELECT i1.i+s1.i)=(SELECT i1.i+s2.i)) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {6, 6, 6, 6}));
	// explicit join on subquery
	result = con.Query("SELECT i, (SELECT SUM(s1.i) FROM integers s1 INNER JOIN integers s2 ON (SELECT i1.i+s1.i)=(SELECT i1.i+s2.i)) AS j FROM integers i1 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6, 6, 6}));
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
