#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test uncorrelated subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

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
	result =
	    con.Query("SELECT * FROM integers WHERE i=(SELECT i FROM integers WHERE i IS NOT NULL ORDER BY i LIMIT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// returning multiple columns should fail though
	REQUIRE_FAIL(con.Query("SELECT * FROM integers WHERE i=(SELECT 1, 2)"));
	REQUIRE_FAIL(con.Query("SELECT * FROM integers WHERE i=(SELECT i, i + 2 FROM integers)"));
	// but not for EXISTS queries!
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM integers WHERE EXISTS (SELECT 1, 2)"));
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM integers WHERE EXISTS (SELECT i, i + 2 FROM integers)"));
	// SELECT * should be fine if the star only expands to a single column
	result = con.Query("SELECT (SELECT * FROM integers WHERE i=1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// but not if the star expands to more than one column!
	REQUIRE_FAIL(con.Query("SELECT (SELECT * FROM integers i1, integers i2)"));

	//  uncorrelated subquery in SELECT
	result = con.Query("SELECT (SELECT i FROM integers WHERE i=1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT * FROM integers WHERE i > (SELECT i FROM integers WHERE i=1)");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
}

TEST_CASE("Test uncorrelated exists subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableProfiling();
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

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
}

TEST_CASE("Test uncorrelated ANY subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableProfiling();
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

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
	result = con.Query("SELECT i, i = ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, "
	                   "integers i5, integers i6 WHERE i1.i IS NOT NULL) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, true}));
	result = con.Query("SELECT i, i = ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, "
	                   "integers i5, integers i6 WHERE i1.i IS NOT NULL AND i1.i <> 2) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, false, true}));
	result = con.Query("SELECT i, i >= ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, "
	                   "integers i5, integers i6 WHERE i1.i IS NOT NULL) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), true, true, true}));
	result =
	    con.Query("SELECT i, i >= ANY(SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers "
	              "i5, integers i6 WHERE i1.i IS NOT NULL AND i1.i <> 1 LIMIT 1) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, true, true}));
}

TEST_CASE("Test uncorrelated ALL subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableProfiling();
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

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
	result = con.Query("SELECT i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL) AS k, SUM(i) FROM integers GROUP "
	                   "BY k ORDER BY k;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 3}));

	result = con.Query(
	    "SELECT SUM(i) FROM integers GROUP BY (i >= ALL(SELECT i FROM integers WHERE i IS NOT NULL)) ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 3, 3}));

	result = con.Query("SELECT i >= ALL(SELECT MIN(i) FROM integers WHERE i IS NOT NULL) AS k, SUM(i) FROM integers "
	                   "GROUP BY k ORDER BY k;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 6}));

	// subquery in CASE statement
	result = con.Query("SELECT i, SUM(CASE WHEN (i >= ALL(SELECT i FROM integers WHERE i=2)) THEN 1 ELSE 0 END) FROM "
	                   "integers GROUP BY i ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 0, 1, 1}));

	// subquery in HAVING
	result =
	    con.Query("SELECT i % 2 AS k, SUM(i) FROM integers GROUP BY k HAVING SUM(i) > (SELECT MAX(i) FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {4}));

	result = con.Query("SELECT i FROM integers WHERE NOT(i IN (SELECT i FROM integers WHERE i>1));");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// multiple subqueries in select without FROM
	result = con.Query("SELECT (SELECT SUM(i) FROM integers), (SELECT 42)");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {42}));
}

TEST_CASE("Test uncorrelated VARCHAR subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableProfiling();
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// varchar tests
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello'), ('world'), (NULL)"));
	// ANY
	result = con.Query("SELECT NULL IN (SELECT * FROM strings)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT 'hello' IN (SELECT * FROM strings)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT 'bla' IN (SELECT * FROM strings)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT 'bla' IN (SELECT * FROM strings WHERE v IS NOT NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	// EXISTS
	result = con.Query("SELECT * FROM strings WHERE EXISTS(SELECT NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world", Value()}));
	result = con.Query("SELECT * FROM strings WHERE EXISTS(SELECT v FROM strings WHERE v='bla')");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// scalar query
	result = con.Query("SELECT (SELECT v FROM strings WHERE v='hello') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "hello", "hello"}));
	result = con.Query("SELECT (SELECT v FROM strings WHERE v='bla') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
}

TEST_CASE("Test subqueries from the paper 'Unnesting Arbitrary Subqueries'", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
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
