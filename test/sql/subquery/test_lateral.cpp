#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test lateral joins", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	return;
	// not supported yet

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE students(id INTEGER, name VARCHAR, major VARCHAR, year INTEGER)"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE exams(sid INTEGER, course VARCHAR, curriculum VARCHAR, grade INTEGER, year INTEGER)"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO students VALUES (1, 'Mark', 'CS', 2017)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO students VALUES (2, 'Dirk', 'CS', 2017)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO exams VALUES (1, 'Database Systems', 'CS', 10, 2015)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO exams VALUES (1, 'Graphics', 'CS', 9, 2016)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO exams VALUES (2, 'Database Systems', 'CS', 7, 2015)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO exams VALUES (2, 'Graphics', 'CS', 7, 2016)"));

	// // lateral join with explicit LATERAL added
	// result = con.Query("SELECT name, total FROM students LEFT JOIN LATERAL (SELECT SUM(grade) AS total FROM exams
	// WHERE exams.sid=students.id) grades ON true ORDER BY total DESC;"); REQUIRE(CHECK_COLUMN(result, 0, {"Mark",
	// "Dirk"})); REQUIRE(CHECK_COLUMN(result, 1, {19, 14}));

	// lateral join without explicit LATERAL
	result = con.Query("SELECT name, total FROM students, (SELECT SUM(grade) AS total FROM exams WHERE "
	                   "exams.sid=students.id) grades ORDER BY total DESC;");
	REQUIRE(CHECK_COLUMN(result, 0, {"Mark", "Dirk"}));
	REQUIRE(CHECK_COLUMN(result, 1, {19, 14}));
}
