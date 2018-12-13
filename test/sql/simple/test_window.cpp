#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Most basic window function", "[window]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE empsalary (depname varchar, empno bigint, salary int, enroll_date date)"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO empsalary VALUES ('develop', 10, 5200, '2007-08-01'), ('sales', 1, 5000, '2006-10-01'), "
	              "('personnel', 5, 3500, '2007-12-10'), ('sales', 4, 4800, '2007-08-08'), ('personnel', 2, 3900, "
	              "'2006-12-23'), ('develop', 7, 4200, '2008-01-01'), ('develop', 9, 4500, '2008-01-01'), ('sales', 3, "
	              "4800, '2007-08-01'), ('develop', 8, 6000, '2006-10-01'), ('develop', 11, 5200, '2007-08-15')"));

	result = con.Query("SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname ORDER BY empno) FROM "
	                   "empsalary ORDER BY depname, empno");
	REQUIRE(CHECK_COLUMN(
	    result, 0,
	    {"develop", "develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1, {7, 8, 9, 10, 11, 2, 5, 1, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {4200, 6000, 4500, 5200, 5200, 3900, 3500, 5000, 4800, 4800}));
	REQUIRE(CHECK_COLUMN(result, 3, {4200, 10200, 14700, 19900, 25100, 3900, 7400, 5000, 9800, 14600}));

	result = con.Query(
	    "SELECT sum(salary) OVER (PARTITION BY depname ORDER BY salary) ss FROM empsalary ORDER BY depname, ss");
	REQUIRE(result->column_count() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {4200, 8700, 13900, 19100, 25100, 3500, 7400, 4800, 9600, 14600}));

	result = con.Query(
	    "SELECT row_number() OVER (PARTITION BY depname ORDER BY salary) rn FROM empsalary ORDER BY depname, rn");
	REQUIRE(result->column_count() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 1, 2, 1, 2, 3}));
}
