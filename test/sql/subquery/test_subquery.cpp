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

	result = con.Query("SELECT * FROM integers WHERE i=(SELECT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT * FROM integers WHERE i=(SELECT SUM(1))");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT * FROM integers WHERE i=(SELECT MIN(i) FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT * FROM integers WHERE i=(SELECT MAX(i) FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// controversial, in postgres this gives an error (and "officially" it should)
	// but SQLite accepts it and just uses the first value
	// we choose to agree with SQLite here
	result = con.Query("SELECT * FROM integers WHERE i=(SELECT i FROM integers WHERE i IS NOT NULL ORDER BY i)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// i.e. the above query is equivalent to this query
	result = con.Query("SELECT * FROM integers WHERE i=(SELECT i FROM integers WHERE i IS NOT NULL ORDER BY i LIMIT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// FIXME:
	// returning multiple columns should fail though
	// REQUIRE_FAIL(con.Query("SELECT * FROM integers WHERE i=(SELECT 1, 2)"));
	// REQUIRE_FAIL(con.Query("SELECT * FROM integers WHERE i=(SELECT i, i + 2 FROM integers)"));

	// uncorrelated EXISTS
	result = con.Query("SELECT * FROM integers WHERE EXISTS(SELECT 1) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	result = con.Query("SELECT * FROM integers WHERE EXISTS(SELECT * FROM integers) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	result = con.Query("SELECT * FROM integers WHERE EXISTS(SELECT NULL) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));

	// uncorrelated IN
	result = con.Query("SELECT * FROM integers WHERE 1 IN (SELECT 1) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	result = con.Query("SELECT * FROM integers WHERE 1 IN (SELECT * FROM integers) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	result = con.Query("SELECT * FROM integers WHERE 1 IN (SELECT NULL) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
}

TEST_CASE("Test subqueries from the paper 'Unnesting Arbitrary Subqueries'", "[subquery]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.EnableQueryVerification();

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
