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

	// basic example from postgres' window.sql
	result = con.Query("SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname ORDER BY empno) FROM "
	                   "empsalary ORDER BY depname, empno");
	REQUIRE(CHECK_COLUMN(
	    result, 0,
	    {"develop", "develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1, {7, 8, 9, 10, 11, 2, 5, 1, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {4200, 6000, 4500, 5200, 5200, 3900, 3500, 5000, 4800, 4800}));
	REQUIRE(CHECK_COLUMN(result, 3, {4200, 10200, 14700, 19900, 25100, 3900, 7400, 5000, 9800, 14600}));

	// sum
	result = con.Query(
	    "SELECT sum(salary) OVER (PARTITION BY depname ORDER BY salary) ss FROM empsalary ORDER BY depname, ss");
	REQUIRE(result->column_count() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {4200, 8700, 13900, 19100, 25100, 3500, 7400, 4800, 9600, 14600}));

	// row_number
	result = con.Query(
	    "SELECT row_number() OVER (PARTITION BY depname ORDER BY salary) rn FROM empsalary ORDER BY depname, rn");
	REQUIRE(result->column_count() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 1, 2, 1, 2, 3}));

	// first_value
	result = con.Query("SELECT empno, first_value(empno) OVER (PARTITION BY depname ORDER BY empno) fv FROM empsalary "
	                   "ORDER BY depname, fv");
	REQUIRE(result->column_count() == 2);
	REQUIRE(CHECK_COLUMN(result, 0, {11, 8, 7, 9, 10, 5, 2, 4, 3, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {7, 7, 7, 7, 7, 2, 2, 1, 1, 1}));

	// rank_dense
	result = con.Query("SELECT depname, salary, dense_rank() OVER (PARTITION BY depname ORDER BY salary) FROM "
	                   "empsalary order by depname, salary");
	REQUIRE(result->column_count() == 3);
	REQUIRE(CHECK_COLUMN(
	    result, 0,
	    {"develop", "develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1, {4200, 4500, 5200, 5200, 6000, 3500, 3900, 4800, 4800, 5000}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 3, 4, 1, 2, 1, 1, 2}));

	// rank
	result = con.Query("SELECT depname, salary, rank() OVER (PARTITION BY depname ORDER BY salary) FROM "
	                   "empsalary order by depname, salary");
	REQUIRE(result->column_count() == 3);
	REQUIRE(CHECK_COLUMN(
	    result, 0,
	    {"develop", "develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1, {4200, 4500, 5200, 5200, 6000, 3500, 3900, 4800, 4800, 5000}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 3, 5, 1, 2, 1, 1, 3}));
}

TEST_CASE("More evil cases", "[window]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE empsalary (depname varchar, empno bigint, salary int, enroll_date date)"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO empsalary VALUES ('develop', 10, 5200, '2007-08-01'), ('sales', 1, 5000, '2006-10-01'), "
	              "('personnel', 5, 3500, '2007-12-10'), ('sales', 4, 4800, '2007-08-08'), ('personnel', 2, 3900, "
	              "'2006-12-23'), ('develop', 7, 4200, '2008-01-01'), ('develop', 9, 4500, '2008-01-01'), ('sales', 3, "
	              "4800, '2007-08-01'), ('develop', 8, 6000, '2006-10-01'), ('develop', 11, 5200, '2007-08-15')"));

	// aggr as input to window
	result = con.Query("SELECT depname, sum(sum(salary)) over (partition by depname order by salary) FROM empsalary "
	                   "group by depname, salary order by depname, salary");
	REQUIRE(result->column_count() == 2);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1, {4200, 8700, 19100, 25100, 3500, 7400, 9600, 14600}));

	// expr in window
	result = con.Query("SELECT empno, sum(salary*2) OVER (PARTITION BY depname ORDER BY empno) FROM "
	                   "empsalary ORDER BY depname, empno");
	REQUIRE(result->column_count() == 2);
	REQUIRE(CHECK_COLUMN(result, 0, {7, 8, 9, 10, 11, 2, 5, 1, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {8400, 20400, 29400, 39800, 50200, 7800, 14800, 10000, 19600, 29200}));

	// expr ontop of window
	result = con.Query("SELECT empno, 2*sum(salary) OVER (PARTITION BY depname ORDER BY empno) FROM "
	                   "empsalary ORDER BY depname, empno");
	REQUIRE(result->column_count() == 2);
	REQUIRE(CHECK_COLUMN(result, 0, {7, 8, 9, 10, 11, 2, 5, 1, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {8400, 20400, 29400, 39800, 50200, 7800, 14800, 10000, 19600, 29200}));

	// tpcds-derived window
	result =
	    con.Query("SELECT depname, sum(salary)*100.0000/sum(sum(salary)) OVER (PARTITION BY depname ORDER BY salary) "
	              "AS revenueratio FROM empsalary GROUP BY depname, salary ORDER BY depname, revenueratio");
	REQUIRE(result->column_count() == 2);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {23.904382, 51.724138, 54.450262, 100.000000, 52.702703, 100.000000, 34.246575, 100.000000}));
}

TEST_CASE("Wiscosin-derived window test cases", "[window]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tenk1 (unique1 int4, unique2 int4, two int4, four int4, ten int4, twenty "
	                          "int4, hundred int4, thousand int4, twothousand int4, fivethous int4, tenthous int4, odd "
	                          "int4, even int4, stringu1 varchar, stringu2 varchar, string4 varchar)"));
	REQUIRE_NO_FAIL(
	    con.Query("insert into tenk1 values (8800,0,0,0,0,0,0,800,800,3800,8800,0,1,'MAAAAA','AAAAAA','AAAAxx'), "
	              "(1891,1,1,3,1,11,91,891,1891,1891,1891,182,183,'TUAAAA','BAAAAA','HHHHxx'), "
	              "(3420,2,0,0,0,0,20,420,1420,3420,3420,40,41,'OBAAAA','CAAAAA','OOOOxx'), "
	              "(9850,3,0,2,0,10,50,850,1850,4850,9850,100,101,'WOAAAA','DAAAAA','VVVVxx'), "
	              "(7164,4,0,0,4,4,64,164,1164,2164,7164,128,129,'OPAAAA','EAAAAA','AAAAxx'), "
	              "(8009,5,1,1,9,9,9,9,9,3009,8009,18,19,'BWAAAA','FAAAAA','HHHHxx'), "
	              "(5057,6,1,1,7,17,57,57,1057,57,5057,114,115,'NMAAAA','GAAAAA','OOOOxx'), "
	              "(6701,7,1,1,1,1,1,701,701,1701,6701,2,3,'TXAAAA','HAAAAA','VVVVxx'), "
	              "(4321,8,1,1,1,1,21,321,321,4321,4321,42,43,'FKAAAA','IAAAAA','AAAAxx'), "
	              "(3043,9,1,3,3,3,43,43,1043,3043,3043,86,87,'BNAAAA','JAAAAA','HHHHxx')"));

	result = con.Query("SELECT sum(four) OVER (PARTITION BY ten ORDER BY unique2) AS sum_1, ten, four FROM tenk1 WHERE "
	                   "unique2 < 10 order by ten, unique2");

	REQUIRE(result->column_count() == 3);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 2, 3, 4, 5, 3, 0, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 0, 0, 1, 1, 1, 3, 4, 7, 9}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 0, 2, 3, 1, 1, 3, 0, 1, 1}));

	result = con.Query("SELECT row_number() OVER (ORDER BY unique2) rn FROM tenk1 WHERE unique2 < 10 ORDER BY rn");
	REQUIRE(result->column_count() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));

	result = con.Query("SELECT rank() OVER (PARTITION BY four ORDER BY ten) AS rank_1, ten, four FROM tenk1 WHERE "
	                   "unique2 < 10 ORDER BY four, ten");
	REQUIRE(result->column_count() == 3);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 3, 1, 1, 3, 4, 1, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 0, 4, 1, 1, 7, 9, 0, 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 0, 0, 1, 1, 1, 1, 2, 3, 3}));

	result = con.Query("SELECT dense_rank() OVER (PARTITION BY four ORDER BY ten) FROM tenk1 WHERE unique2 "
	                   "< 10 ORDER BY four, ten");
	REQUIRE(result->column_count() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 2, 1, 1, 2, 3, 1, 1, 2}));

	result = con.Query("SELECT first_value(ten) OVER (PARTITION BY four ORDER BY ten) FROM tenk1 WHERE unique2 < 10 "
	                   "order by four, ten");
	REQUIRE(result->column_count() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, 1, 1, 1, 1, 0, 1, 1}));
}
