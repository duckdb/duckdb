#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Most scalar window functions", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// test scalar window functions
	result = con.Query("SELECT row_number() OVER ()");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT avg(42) OVER ()");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// nested window functions are not allowed
	REQUIRE_FAIL(con.Query("SELECT avg(row_number() over ()) over ()"));
	REQUIRE_FAIL(con.Query("SELECT avg(42) over (partition by row_number() over ())"));
	REQUIRE_FAIL(con.Query("SELECT avg(42) over (order by row_number() over ())"));
	// distinct aggregates not supported for window functions
	REQUIRE_FAIL(con.Query("SELECT COUNT(DISTINCT 42) OVER ()"));
	REQUIRE_FAIL(
	    con.Query("WITH t AS (SELECT col0 AS a, col1 AS b FROM (VALUES(1,2),(1,1),(1,2),(2,1),(2,1),(2,2),(2,3),(2,4)) "
	              "v) SELECT *, COUNT(b) OVER(PARTITION BY a), COUNT(DISTINCT b) OVER(PARTITION BY a) FROM t;"));
}

TEST_CASE("Most basic window function", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

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
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {4200, 8700, 19100, 19100, 25100, 3500, 7400, 9600, 9600, 14600}));

	// row_number
	result = con.Query(
	    "SELECT row_number() OVER (PARTITION BY depname ORDER BY salary) rn FROM empsalary ORDER BY depname, rn");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 1, 2, 1, 2, 3}));

	// first_value
	result = con.Query("SELECT empno, first_value(empno) OVER (PARTITION BY depname ORDER BY empno) fv FROM empsalary "
	                   "ORDER BY depname, fv");
	REQUIRE(result->types.size() == 2);
	REQUIRE(CHECK_COLUMN(result, 0, {11, 8, 7, 9, 10, 5, 2, 4, 3, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {7, 7, 7, 7, 7, 2, 2, 1, 1, 1}));

	// rank_dense
	result = con.Query("SELECT depname, salary, dense_rank() OVER (PARTITION BY depname ORDER BY salary) FROM "
	                   "empsalary order by depname, salary");
	REQUIRE(result->types.size() == 3);
	REQUIRE(CHECK_COLUMN(
	    result, 0,
	    {"develop", "develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1, {4200, 4500, 5200, 5200, 6000, 3500, 3900, 4800, 4800, 5000}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 3, 4, 1, 2, 1, 1, 2}));

	// rank
	result = con.Query("SELECT depname, salary, rank() OVER (PARTITION BY depname ORDER BY salary) FROM "
	                   "empsalary order by depname, salary");
	REQUIRE(result->types.size() == 3);
	REQUIRE(CHECK_COLUMN(
	    result, 0,
	    {"develop", "develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1, {4200, 4500, 5200, 5200, 6000, 3500, 3900, 4800, 4800, 5000}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 3, 5, 1, 2, 1, 1, 3}));

	// min/max/avg
	result = con.Query("SELECT depname, min(salary) OVER (PARTITION BY depname ORDER BY salary, empno) m1, max(salary) "
	                   "OVER (PARTITION BY depname ORDER BY salary, empno) m2, AVG(salary) OVER (PARTITION BY depname "
	                   "ORDER BY salary, empno) m3 FROM empsalary ORDER BY depname, empno");
	REQUIRE(result->types.size() == 4);
	REQUIRE(CHECK_COLUMN(
	    result, 0,
	    {"develop", "develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1, {4200, 4200, 4200, 4200, 4200, 3500, 3500, 4800, 4800, 4800}));
	REQUIRE(CHECK_COLUMN(result, 2, {4200, 6000, 4500, 5200, 5200, 3900, 3500, 5000, 4800, 4800}));
	REQUIRE(CHECK_COLUMN(
	    result, 3,
	    {4200.0, 5020.0, 4350.0, 4633.33333333333, 4775.0, 3700.0, 3500.0, 4866.66666666667, 4800.0, 4800.0}));

	// stddev_pop
	result = con.Query("SELECT depname, "
	                   "STDDEV_POP(salary) OVER (PARTITION BY depname ORDER BY salary, empno) s "
	                   "FROM empsalary ORDER BY depname, empno");
	REQUIRE(result->types.size() == 2);
	REQUIRE(CHECK_COLUMN(
	    result, 0,
	    {"develop", "develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1, {0.0, 627.375486, 150.0, 418.993503, 438.035387, 200, 0.0, 94.280904, 0.0, 0.0}));

	// covar_pop
	result = con.Query("SELECT depname, "
	                   "COVAR_POP(salary, empno) OVER (PARTITION BY depname ORDER BY salary, empno) c "
	                   "FROM empsalary ORDER BY depname, empno");
	REQUIRE(result->types.size() == 2);
	REQUIRE(CHECK_COLUMN(
	    result, 0,
	    {"develop", "develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1, {0.0, 240.0, 150.0, 477.777778, 606.250000, -300.0, 0.0, -111.111111, 0.0, 0.0}));
}

TEST_CASE("Illegal window function", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE empsalary (depname varchar, empno bigint, salary int, enroll_date date)"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO empsalary VALUES ('develop', 10, 5200, '2007-08-01'), ('sales', 1, 5000, '2006-10-01'), "
	              "('personnel', 5, 3500, '2007-12-10'), ('sales', 4, 4800, '2007-08-08'), ('personnel', 2, 3900, "
	              "'2006-12-23'), ('develop', 7, 4200, '2008-01-01'), ('develop', 9, 4500, '2008-01-01'), ('sales', 3, "
	              "4800, '2007-08-01'), ('develop', 8, 6000, '2006-10-01'), ('develop', 11, 5200, '2007-08-15')"));

	// GROUP BY window function is not allowed
	REQUIRE_FAIL(con.Query("SELECT depname, min(salary) OVER (PARTITION BY depname ORDER BY salary, empno) m1 FROM "
	                       "empsalary GROUP BY m1 ORDER BY depname, empno"));
}

TEST_CASE("More evil cases", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE empsalary (depname varchar, empno bigint, salary int, enroll_date date)"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO empsalary VALUES ('develop', 10, 5200, '2007-08-01'), ('sales', 1, 5000, '2006-10-01'), "
	              "('personnel', 5, 3500, '2007-12-10'), ('sales', 4, 4800, '2007-08-08'), ('personnel', 2, 3900, "
	              "'2006-12-23'), ('develop', 7, 4200, '2008-01-01'), ('develop', 9, 4500, '2008-01-01'), ('sales', 3, "
	              "4800, '2007-08-01'), ('develop', 8, 6000, '2006-10-01'), ('develop', 11, 5200, '2007-08-15')"));

	// aggr as input to window
	result = con.Query("SELECT depname, sum(sum(salary)) over (partition by depname order by salary) FROM empsalary "
	                   "group by depname, salary order by depname, salary");
	REQUIRE(result->types.size() == 2);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1, {4200, 8700, 19100, 25100, 3500, 7400, 9600, 14600}));

	// expr in window
	result = con.Query("SELECT empno, sum(salary*2) OVER (PARTITION BY depname ORDER BY empno) FROM "
	                   "empsalary ORDER BY depname, empno");
	REQUIRE(result->types.size() == 2);
	REQUIRE(CHECK_COLUMN(result, 0, {7, 8, 9, 10, 11, 2, 5, 1, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {8400, 20400, 29400, 39800, 50200, 7800, 14800, 10000, 19600, 29200}));

	// expr ontop of window
	result = con.Query("SELECT empno, 2*sum(salary) OVER (PARTITION BY depname ORDER BY empno) FROM "
	                   "empsalary ORDER BY depname, empno");
	REQUIRE(result->types.size() == 2);
	REQUIRE(CHECK_COLUMN(result, 0, {7, 8, 9, 10, 11, 2, 5, 1, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {8400, 20400, 29400, 39800, 50200, 7800, 14800, 10000, 19600, 29200}));

	// tpcds-derived window
	result =
	    con.Query("SELECT depname, sum(salary)*100.0000/sum(sum(salary)) OVER (PARTITION BY depname ORDER BY salary) "
	              "AS revenueratio FROM empsalary GROUP BY depname, salary ORDER BY depname, revenueratio");
	REQUIRE(result->types.size() == 2);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"develop", "develop", "develop", "develop", "personnel", "personnel", "sales", "sales"}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {23.904382, 51.724138, 54.450262, 100.000000, 52.702703, 100.000000, 34.246575, 100.000000}));
}

TEST_CASE("Wisconsin-derived window test cases", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

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

	result = con.Query("SELECT COUNT(*) OVER () FROM tenk1");
	REQUIRE(result->types.size() == 1);

	// FIXME	REQUIRE(CHECK_COLUMN(result, 0, {10, 10, 10, 10, 10, 10, 10, 10, 10, 10}));

	result = con.Query("SELECT sum(four) OVER (PARTITION BY ten ORDER BY unique2) AS sum_1, ten, four FROM tenk1 WHERE "
	                   "unique2 < 10 order by ten, unique2");

	REQUIRE(result->types.size() == 3);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 2, 3, 4, 5, 3, 0, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 0, 0, 1, 1, 1, 3, 4, 7, 9}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 0, 2, 3, 1, 1, 3, 0, 1, 1}));

	result = con.Query("SELECT row_number() OVER (ORDER BY unique2) rn FROM tenk1 WHERE unique2 < 10 ORDER BY rn");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));

	result = con.Query("SELECT rank() OVER (PARTITION BY four ORDER BY ten) AS rank_1, ten, four FROM tenk1 WHERE "
	                   "unique2 < 10 ORDER BY four, ten");
	REQUIRE(result->types.size() == 3);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 3, 1, 1, 3, 4, 1, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 0, 4, 1, 1, 7, 9, 0, 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 0, 0, 1, 1, 1, 1, 2, 3, 3}));

	result = con.Query("SELECT dense_rank() OVER (PARTITION BY four ORDER BY ten) FROM tenk1 WHERE unique2 "
	                   "< 10 ORDER BY four, ten");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 2, 1, 1, 2, 3, 1, 1, 2}));

	result = con.Query("SELECT first_value(ten) OVER (PARTITION BY four ORDER BY ten) FROM tenk1 WHERE unique2 < 10 "
	                   "order by four, ten");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, 1, 1, 1, 1, 0, 1, 1}));

	// percent_rank
	result = con.Query("SELECT cast(percent_rank() OVER (PARTITION BY four ORDER BY ten)*10 as INTEGER) FROM tenk1 "
	                   "ORDER BY four, ten");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 10, 0, 0, 6, 10, 0, 0, 10}));

	// cume_dist
	result = con.Query("SELECT cast(cume_dist() OVER (PARTITION BY four ORDER BY ten)*10 as integer) FROM tenk1 WHERE  "
	                   "unique2 < 10 order by four, ten");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {6, 6, 10, 5, 5, 7, 10, 10, 5, 10}));

	// ntile
	result = con.Query("SELECT ntile(2) OVER (ORDER BY ten, four) nn FROM tenk1 ORDER BY ten, four, nn");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1, 1, 2, 2, 2, 2, 2}));

	result = con.Query("SELECT ntile(3) OVER (ORDER BY ten, four) nn FROM tenk1 ORDER BY ten, four, nn");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1, 2, 2, 2, 3, 3, 3}));

	result = con.Query("SELECT ntile(4) OVER (ORDER BY ten, four) nn FROM tenk1 ORDER BY ten, four, nn");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 2, 2, 2, 3, 3, 4, 4}));

	result = con.Query("SELECT ntile(5) OVER (ORDER BY ten, four) nn FROM tenk1 ORDER BY ten, four, nn");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 2, 2, 3, 3, 4, 4, 5, 5}));

	// lead/lag
	result = con.Query("SELECT lag(ten) OVER (PARTITION BY four ORDER BY ten) lt FROM tenk1 order by four, ten, lt");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0, 0, Value(), 1, 1, 7, Value(), Value(), 1}));

	result = con.Query("SELECT lead(ten) OVER (PARTITION BY four ORDER BY ten) lt FROM tenk1 order by four, ten, lt");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 4, Value(), 1, 7, 9, Value(), Value(), 3, Value()}));

	result =
	    con.Query("SELECT lag(ten, four) OVER (PARTITION BY four ORDER BY ten) lt FROM tenk1 order by four, ten, lt");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 4, Value(), 1, 1, 7, Value(), Value(), Value()}));

	result = con.Query(
	    "SELECT lag(ten, four, 0) OVER (PARTITION BY four ORDER BY ten) lt FROM tenk1 order by four, ten, lt");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 4, 0, 1, 1, 7, 0, 0, 0}));

	result = con.Query("SELECT lead(ten) OVER (PARTITION BY four ORDER BY ten) lt FROM tenk1 order by four, ten, lt");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 4, Value(), 1, 7, 9, Value(), Value(), 3, Value()}));

	result =
	    con.Query("SELECT lead(ten * 2, 1) OVER (PARTITION BY four ORDER BY ten) lt FROM tenk1 order by four, ten, lt");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 8, Value(), 2, 14, 18, Value(), Value(), 6, Value()}));

	result = con.Query(
	    "SELECT lead(ten * 2, 1, -1) OVER (PARTITION BY four ORDER BY ten) lt FROM tenk1 order by four, ten, lt");
	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 8, -1, 2, 14, 18, -1, -1, 6, -1}));

	// empty OVER clause
	result =
	    con.Query("SELECT COUNT(*) OVER w c, SUM(four) OVER w s, cast(AVG(ten) OVER w * 10 as integer) a, RANK() over "
	              "w r, DENSE_RANK() over w dr, ROW_NUMBER() OVER w rn FROM tenk1 WINDOW w AS () ORDER BY rn");
	REQUIRE(result->types.size() == 6);
	REQUIRE(CHECK_COLUMN(result, 0, {10, 10, 10, 10, 10, 10, 10, 10, 10, 10}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 12, 12, 12, 12, 12, 12, 12, 12, 12}));
	REQUIRE(CHECK_COLUMN(result, 2, {26, 26, 26, 26, 26, 26, 26, 26, 26, 26}));
	REQUIRE(CHECK_COLUMN(result, 3, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 4, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 5, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));

	// no ordering but still a frame spec (somewhat underdefined)
	result =
	    con.Query("SELECT COUNT(*) OVER w c, SUM(four) OVER w s, cast(AVG(ten) OVER w * 10 as integer) a, RANK() over "
	              "w r, DENSE_RANK() over w dr, ROW_NUMBER() OVER w rn FROM tenk1 WINDOW w AS (rows between 1 "
	              "preceding and 1 following) ORDER BY rn");
	REQUIRE(result->types.size() == 6);
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 3, 3, 3, 3, 3, 3, 3, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3, 5, 2, 3, 2, 3, 3, 5, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {5, 3, 3, 13, 43, 66, 56, 30, 16, 20}));
	REQUIRE(CHECK_COLUMN(result, 3, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 4, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 5, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
}

TEST_CASE("Non-default window specs", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table tenk1d(ten int4, four int4)"));
	REQUIRE_NO_FAIL(con.Query("insert into tenk1d values (0,0), (1,1), (3,3), (2,2), (4,2), (9,1), (4,0), (7,3), "
	                          "(0,2), (2,0), (5,1), (1,3), (3,1), (6,0), (8,0), (9,3), (8,2), (6,2), (7,1), (5,3)"));

	// BASIC
	result = con.Query("SELECT four, ten, sum(ten) over (partition by four order by ten) st, last_value(ten) over "
	                   "(partition by four order by ten) lt FROM tenk1d ORDER BY four, ten");
	REQUIRE(result->types.size() == 4);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 2, 4, 6, 8, 1, 3, 5, 7, 9, 0, 2, 4, 6, 8, 1, 3, 5, 7, 9}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 2, 6, 12, 20, 1, 4, 9, 16, 25, 0, 2, 6, 12, 20, 1, 4, 9, 16, 25}));
	REQUIRE(CHECK_COLUMN(result, 3, {0, 2, 4, 6, 8, 1, 3, 5, 7, 9, 0, 2, 4, 6, 8, 1, 3, 5, 7, 9}));

	// same but with explicit window def
	result = con.Query("SELECT four, ten, sum(ten) over (partition by four order by ten range between unbounded "
	                   "preceding and current row) st, last_value(ten) over (partition by four order by ten range "
	                   "between unbounded preceding and current row) lt FROM tenk1d order by four, ten");
	REQUIRE(result->types.size() == 4);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 2, 4, 6, 8, 1, 3, 5, 7, 9, 0, 2, 4, 6, 8, 1, 3, 5, 7, 9}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 2, 6, 12, 20, 1, 4, 9, 16, 25, 0, 2, 6, 12, 20, 1, 4, 9, 16, 25}));
	REQUIRE(CHECK_COLUMN(result, 3, {0, 2, 4, 6, 8, 1, 3, 5, 7, 9, 0, 2, 4, 6, 8, 1, 3, 5, 7, 9}));

	// unbounded following
	result = con.Query("SELECT four, ten, sum(ten) over (partition by four order by ten range between unbounded "
	                   "preceding and unbounded following) st, last_value(ten) over (partition by four order by ten "
	                   "range between unbounded preceding and unbounded following) lt FROM tenk1d order by four, ten");
	REQUIRE(result->types.size() == 4);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 2, 4, 6, 8, 1, 3, 5, 7, 9, 0, 2, 4, 6, 8, 1, 3, 5, 7, 9}));
	REQUIRE(CHECK_COLUMN(result, 2, {20, 20, 20, 20, 20, 25, 25, 25, 25, 25, 20, 20, 20, 20, 20, 25, 25, 25, 25, 25}));
	REQUIRE(CHECK_COLUMN(result, 3, {8, 8, 8, 8, 8, 9, 9, 9, 9, 9, 8, 8, 8, 8, 8, 9, 9, 9, 9, 9}));

	// unbounded following with expressions
	result = con.Query("SELECT four, ten/4 as two, 	sum(ten/4) over (partition by four order by ten/4 range between "
	                   "unbounded preceding and current row) st, last_value(ten/4) over (partition by four order by "
	                   "ten/4 range between unbounded preceding and current row) lt FROM tenk1d order by four, ten/4");
	REQUIRE(result->types.size() == 4);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 0, 1, 1, 2, 0, 0, 1, 1, 2, 0, 0, 1, 1, 2, 0, 0, 1, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 0, 2, 2, 4, 0, 0, 2, 2, 4, 0, 0, 2, 2, 4, 0, 0, 2, 2, 4}));
	REQUIRE(CHECK_COLUMN(result, 3, {0, 0, 1, 1, 2, 0, 0, 1, 1, 2, 0, 0, 1, 1, 2, 0, 0, 1, 1, 2}));

	// unbounded following with named windows
	result = con.Query(
	    "SELECT four, ten/4 as two, sum(ten/4) OVER w st, last_value(ten/4) OVER w lt FROM tenk1d WINDOW w AS "
	    "(partition by four order by ten/4 range between unbounded preceding and current row) order by four, ten/4 ");
	REQUIRE(result->types.size() == 4);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 0, 1, 1, 2, 0, 0, 1, 1, 2, 0, 0, 1, 1, 2, 0, 0, 1, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 0, 2, 2, 4, 0, 0, 2, 2, 4, 0, 0, 2, 2, 4, 0, 0, 2, 2, 4}));
	REQUIRE(CHECK_COLUMN(result, 3, {0, 0, 1, 1, 2, 0, 0, 1, 1, 2, 0, 0, 1, 1, 2, 0, 0, 1, 1, 2}));
}

TEST_CASE("Expressions in boundaries", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tenk1 ( unique1 int4, unique2 int4, two int4, four int4, ten int4, twenty "
	                          "int4, hundred int4, thousand int4, twothousand int4, fivethous int4, tenthous int4, odd "
	                          "int4, even int4, stringu1 string, stringu2 string, string4 string )"));
	REQUIRE_NO_FAIL(
	    con.Query("insert into tenk1 values (4, 1621, 0, 0, 4, 4, 4, 4, 4, 4, 4, 8 ,9 ,'EAAAAA', 'JKCAAA', 'HHHHxx'), "
	              "(2, 2716, 0, 2, 2, 2, 2, 2, 2, 2, 2, 4 ,5 ,'CAAAAA', 'MAEAAA', 'AAAAxx'), (1, 2838, 1, 1, 1, 1, 1, "
	              "1, 1, 1, 1, 2 ,3 ,'BAAAAA', 'EFEAAA', 'OOOOxx'), (6, 2855, 0, 2, 6, 6, 6, 6, 6, 6, 6, 12 ,13 "
	              ",'GAAAAA', 'VFEAAA', 'VVVVxx'), (9, 4463, 1, 1, 9, 9, 9, 9, 9, 9, 9, 18 ,19 ,'JAAAAA', 'RPGAAA', "
	              "'VVVVxx'),(8, 5435, 0, 0, 8, 8, 8, 8, 8, 8, 8, 16 ,17 ,'IAAAAA', 'BBIAAA', 'VVVVxx'), (5, 5557, 1, "
	              "1, 5, 5, 5, 5, 5, 5, 5, 10 ,11,'FAAAAA', 'TFIAAA', 'HHHHxx'), (3, 5679, 1, 3, 3, 3, 3, 3, 3, 3, 3, "
	              "6 ,7 ,'DAAAAA', 'LKIAAA', 'VVVVxx'), (7, 8518, 1,3, 7, 7, 7, 7, 7, 7, 7, 14 ,15 ,'HAAAAA', "
	              "'QPMAAA', 'OOOOxx'), (0, 9998, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ,1 ,'AAAAAA','OUOAAA', 'OOOOxx')"));

	result = con.Query("SELECT sum(unique1) over (order by unique1 rows between 2 preceding and 2 following) su FROM "
	                   "tenk1 order by unique1");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 6, 10, 15, 20, 25, 30, 35, 30, 24}));
	REQUIRE(result->types.size() == 1);

	result = con.Query("SELECT sum(unique1) over (order by unique1 rows between 2 preceding and 1 preceding) su FROM "
	                   "tenk1 order by unique1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0, 1, 3, 5, 7, 9, 11, 13, 15}));
	REQUIRE(result->types.size() == 1);

	result = con.Query("SELECT sum(unique1) over (order by unique1 rows between 1 following and 3 following) su FROM "
	                   "tenk1 order by unique1");
	REQUIRE(CHECK_COLUMN(result, 0, {6, 9, 12, 15, 18, 21, 24, 17, 9, Value()}));
	REQUIRE(result->types.size() == 1);

	result = con.Query("SELECT sum(unique1) over (order by unique1 rows between unbounded preceding and 1 following) "
	                   "su FROM tenk1 order by unique1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3, 6, 10, 15, 21, 28, 36, 45, 45}));
	REQUIRE(result->types.size() == 1);
}

TEST_CASE("TPC-DS inspired micro benchmarks", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE item(i_category VARCHAR, i_brand VARCHAR, i_price INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO item VALUES ('toys', 'fisher-price', 100)"));
	result =
	    con.Query("SELECT i_category, i_brand, avg(sum(i_price)) OVER (PARTITION BY i_category), rank() OVER "
	              "(PARTITION BY i_category ORDER BY i_category, i_brand) rn FROM item GROUP BY i_category, i_brand;");
	REQUIRE(CHECK_COLUMN(result, 0, {"toys"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"fisher-price"}));
	REQUIRE(CHECK_COLUMN(result, 2, {100}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE item(i_category VARCHAR, i_brand VARCHAR, i_item_sk INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE store(s_store_name VARCHAR, s_company_name VARCHAR, s_store_sk INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE date_dim(d_year INTEGER, d_moy INTEGER, d_date_sk INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE store_sales(ss_sales_price DECIMAL, ss_item_sk INTEGER, ss_sold_date_sk "
	                          "INTEGER, ss_store_sk INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO item VALUES ('Music', 'exportischolar', 1);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO store VALUES ('ought', 'Unknown', 1);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO date_dim VALUES (1999, 1, 1);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO store_sales VALUES (2.8, 1, 1, 1);"));
	result = con.Query(
	    "SELECT i_category, i_brand, s_store_name, s_company_name, d_year, d_moy, sum(ss_sales_price) sum_sales, "
	    "avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) "
	    "avg_monthly_sales, rank() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY "
	    "d_year, d_moy) rn FROM item, store_sales, date_dim, store WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk = "
	    "d_date_sk AND ss_store_sk = s_store_sk AND (d_year = 1999 OR (d_year = 1999-1 AND d_moy =12) OR (d_year = "
	    "1999+1 AND d_moy =1)) GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy;");
	REQUIRE(CHECK_COLUMN(result, 0, {"Music"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"exportischolar"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"ought"}));
	REQUIRE(CHECK_COLUMN(result, 3, {"Unknown"}));
	REQUIRE(CHECK_COLUMN(result, 4, {1999}));
	REQUIRE(CHECK_COLUMN(result, 5, {1}));
	REQUIRE(CHECK_COLUMN(result, 6, {2.8}));
	REQUIRE(CHECK_COLUMN(result, 7, {2.8}));
	REQUIRE(CHECK_COLUMN(result, 8, {1}));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}

TEST_CASE("TPC-DS Q49 bug fix for multi-sort window functions", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("create table wintest( item integer, return_ratio numeric, currency_ratio numeric)"));
	REQUIRE_NO_FAIL(con.Query(
	    "insert into wintest values  (7539  ,0.590000 , 0.590000), (3337  ,0.626506 , 0.626506), (15597 ,0.661972 , "
	    "0.661972), (2915  ,0.698630 , 0.698630), (11933 ,0.717172 , 0.717172), (483   ,0.800000 , 0.800000), (85    "
	    ",0.857143 , 0.857143), (97    ,0.903614 , 0.903614), (117   ,0.925000 , 0.925000), (5299  ,0.927083 , "
	    "0.927083), (10055 ,0.945652 , 0.945652), (4231  ,0.977778 , 0.977778), (5647  ,0.987805 , 0.987805), (8679  "
	    ",0.988764 , 0.988764), (10323 ,0.977778 , 1.111111), (3305  ,0.737500 , 1.293860)"));

	result = con.Query("SELECT item, rank() OVER (ORDER BY return_ratio) AS return_rank, rank() OVER (ORDER BY "
	                   "currency_ratio) AS currency_rank FROM wintest order by item");
	REQUIRE(CHECK_COLUMN(
	    result, 0, {85, 97, 117, 483, 2915, 3305, 3337, 4231, 5299, 5647, 7539, 8679, 10055, 10323, 11933, 15597}));
	REQUIRE(CHECK_COLUMN(result, 1, {8, 9, 10, 7, 4, 6, 2, 13, 11, 15, 1, 16, 12, 13, 5, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {7, 8, 9, 6, 4, 16, 2, 12, 10, 13, 1, 14, 11, 15, 5, 3}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}

TEST_CASE("Ensure dbplyr crash with ORDER BY under window stays fixed", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dbplyr_052 (x INTEGER, g DOUBLE, w int)"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO dbplyr_052 VALUES (1,1, 42),(2,1, 42),(3,1, 42),(2,2, 42),(3,2, 42),(4,2, 42)"));

	// this works fine because we order by the already-projected column in the innermost query
	result =
	    con.Query("SELECT x, g FROM (SELECT x, g, SUM(x) OVER (PARTITION BY g ORDER BY x ROWS UNBOUNDED PRECEDING) AS "
	              "zzz67 FROM (SELECT x, g FROM dbplyr_052 ORDER BY x) dbplyr_053) dbplyr_054 WHERE (zzz67 > 3.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {1.0, 2.0, 2.0}));

	// this breaks because we add a fake projection that is not pruned
	result =
	    con.Query("SELECT x, g FROM (SELECT x, g, SUM(x) OVER (PARTITION BY g ORDER BY x ROWS UNBOUNDED PRECEDING) AS "
	              "zzz67 FROM (SELECT x, g FROM dbplyr_052 ORDER BY w) dbplyr_053) dbplyr_054 WHERE (zzz67 > 3.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {1.0, 2.0, 2.0}));

	// this also breaks because we add a fake projection that is not pruned even if we already have that projection,
	// just with a different table name
	result =
	    con.Query("SELECT x, g FROM (SELECT x, g, SUM(x) OVER (PARTITION BY g ORDER BY x ROWS UNBOUNDED PRECEDING) AS "
	              "zzz67 FROM (SELECT * FROM dbplyr_052 ORDER BY x) dbplyr_053) dbplyr_054 WHERE (zzz67 > 3.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {1.0, 2.0, 2.0}));
}

TEST_CASE("Test errors in binding window functions", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));

	// we use columns here that are not part of the table
	REQUIRE_FAIL(con.Query("SELECT MIN(a) OVER (PARTITION BY i ORDER BY i) FROM integers"));
	REQUIRE_FAIL(con.Query("SELECT MIN(i) OVER (PARTITION BY a ORDER BY i) FROM integers"));
	REQUIRE_FAIL(con.Query("SELECT MIN(i) OVER (PARTITION BY i ORDER BY a) FROM integers"));
	REQUIRE_FAIL(con.Query("SELECT MIN(i) OVER (PARTITION BY i, a ORDER BY i) FROM integers"));
	REQUIRE_FAIL(con.Query("SELECT MIN(i) OVER (PARTITION BY i ORDER BY i, a) FROM integers"));

	// now we only use the "proper" columns
	result = con.Query("SELECT MIN(i) OVER (PARTITION BY i ORDER BY i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
}

TEST_CASE("Test binding of named window functions in CTEs", "[window]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// named window clause
	result = con.Query(
	    "select i, lag(i) over named_window from (values (1), (2), (3)) as t (i) window named_window as (order by i);");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2}));

	// named window clause in CTE
	result = con.Query("with subquery as (select i, lag(i) over named_window from (values (1), (2), (3)) as t (i) "
	                   "window named_window as (order by i)) select * from subquery;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2}));

	// named window clause in subquery
	result = con.Query("select * from (select i, lag(i) over named_window from (values (1), (2), (3)) as t (i) window "
	                   "named_window as (order by i)) t1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2}));

	// named window clause in view
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS select i, lag(i) over named_window from (values (1), (2), (3)) as t "
	                          "(i) window named_window as (order by i);"));

	result = con.Query("select * from v1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2}));

	// same window clause name multiple times but in different subqueries
	result = con.Query("SELECT * FROM (SELECT i, lag(i) OVER named_window FROM ( VALUES (1), (2), (3)) AS t (i) window "
	                   "named_window AS ( ORDER BY i)) t1, (SELECT i, lag(i) OVER named_window FROM ( VALUES (1), (2), "
	                   "(3)) AS t (i) window named_window AS ( ORDER BY i)) t2 ORDER BY 1, 2, 3, 4;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 2, 2, 2, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), 1, 1, 1, 2, 2, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 1, 2, 3, 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), 1, 2, Value(), 1, 2, Value(), 1, 2}));

	// we cannot use named window specifications of the main query inside CTEs
	REQUIRE_FAIL(con.Query("WITH subquery AS (SELECT i, lag(i) OVER named_window FROM ( VALUES (1), (2), (3)) AS t "
	                       "(i)) SELECT * FROM subquery window named_window AS ( ORDER BY i);"));
	// duplicate window clause name
	REQUIRE_FAIL(con.Query("select i, lag(i) over named_window from (values (1), (2), (3)) as t (i) window "
	                       "named_window as (order by i), named_window as (order by j);"));
}
