#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test ceil(ing)/floor function", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(n DOUBLE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (NULL),(-42.8),(-42.2),(0), (42.2), (42.8)"));

	result = con.Query("SELECT cast(CEIL(n::tinyint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(CEIL(n::smallint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(CEIL(n::integer) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(CEIL(n::bigint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(CEIL(n::float) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 43, 43}));
	result = con.Query("SELECT cast(CEIL(n::double) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 43, 43}));

	result = con.Query("SELECT cast(CEILING(n::double) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 43, 43}));

	result = con.Query("SELECT cast(FLOOR(n::tinyint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(FLOOR(n::smallint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(FLOOR(n::integer) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(FLOOR(n::bigint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(FLOOR(n::float) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -43, -43, 0, 42, 42}));
	result = con.Query("SELECT cast(FLOOR(n::double) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -43, -43, 0, 42, 42}));
}

TEST_CASE("Rounding test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE roundme(a DOUBLE, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO roundme VALUES (42.123456, 3)"));

	result = con.Query("select round(42.12345, 0)");
	REQUIRE(CHECK_COLUMN(result, 0, {42.0}));

	result = con.Query("select round(42.12345, 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {42.12}));

	result = con.Query("select round(42, 0)");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("select round(a, 1) from roundme");
	REQUIRE(CHECK_COLUMN(result, 0, {42.1}));

	result = con.Query("select round(b, 1) from roundme");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	result = con.Query("select round(a, b) from roundme");
	REQUIRE(CHECK_COLUMN(result, 0, {42.123}));
}

TEST_CASE("Test random & setseed functions", "[function]") {
	unique_ptr<QueryResult> result, result1, result2;
	DuckDB db(nullptr);
	Connection con(db);

	// random() is evaluated twice here
	result = con.Query("select case when random() between 0 and 0.99999 then 1 else 0 end");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result1 = con.Query("select random()");
	result2 = con.Query("select random()");
	REQUIRE(!result1->Equals(*result2));

	REQUIRE_NO_FAIL(con.Query("select setseed(0.1)"));
	result1 = con.Query("select random(), random(), random()");
	REQUIRE(CHECK_COLUMN(result1, 0, {0.612055}));
	REQUIRE(CHECK_COLUMN(result1, 1, {0.384141}));
	REQUIRE(CHECK_COLUMN(result1, 2, {0.288025}));
	REQUIRE_NO_FAIL(con.Query("select setseed(0.1)"));
	result2 = con.Query("select random(), random(), random()");
	REQUIRE(result1->Equals(*result2));

	REQUIRE_FAIL(con.Query("select setseed(1.1)"));
	REQUIRE_FAIL(con.Query("select setseed(-1.1)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE seeds(a DOUBLE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO seeds VALUES (-0.1), (0.0), (0.1)"));
	result2 = con.Query("select setseed(a), a from seeds;");
	REQUIRE(CHECK_COLUMN(result2, 0, {Value(), Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result2, 1, {-0.1, 0.0, 0.1}));
	// Make sure last seed (0.1) is in effect
	result1 = con.Query("select random(), random(), random()");
	REQUIRE_NO_FAIL(con.Query("select setseed(0.1)"));
	result2 = con.Query("select random(), random(), random()");
	REQUIRE(result1->Equals(*result2));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(a INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)"));

	result = con.Query("select case when min(random()) >= 0 then 1 else 0 end from numbers;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("select case when max(random()) < 1 then 1 else 0 end from numbers;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	REQUIRE_NO_FAIL(con.Query("select * from numbers order by random()"));
	REQUIRE_NO_FAIL(con.Query("select random() from numbers"));
}

// see https://www.postgresql.org/docs/10/functions-math.html

TEST_CASE("Function test cases from PG docs", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("select abs(-17.4)");
	REQUIRE(CHECK_COLUMN(result, 0, {17.4}));

	result = con.Query("select cbrt(27.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {3.0}));

	result = con.Query("select ceil(-42.8)");
	REQUIRE(CHECK_COLUMN(result, 0, {-42.0}));

	result = con.Query("select ceiling(-95.3)");
	REQUIRE(CHECK_COLUMN(result, 0, {-95.0}));

	result = con.Query("select exp(1.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {2.71828182845905}));

	result = con.Query("select floor(-42.8)");
	REQUIRE(CHECK_COLUMN(result, 0, {-43.0}));

	result = con.Query("select ln(2.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {0.693147180559945}));

	result = con.Query("select log(100.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {2.0}));

	result = con.Query("select log10(100.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {2.0}));

	result = con.Query("select log2(4.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {2.0}));

	result = con.Query("select pi()");
	REQUIRE(CHECK_COLUMN(result, 0, {3.14159265358979}));

	result = con.Query("select sqrt(2.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {1.4142135623731}));

	result = con.Query("select radians(45.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {0.785398163397448}));

	result = con.Query("select degrees(0.5)");
	REQUIRE(CHECK_COLUMN(result, 0, {28.6478897565412}));

	result = con.Query("select sign(4.1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("select sign(-4.1)");
	REQUIRE(CHECK_COLUMN(result, 0, {-1}));

	result = con.Query("select sign(0)");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	result = con.Query("select sign(3)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Mod test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE modme(a DOUBLE, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO modme VALUES (42.123456, 3)"));

	// input is real, divisor is an integer
	result = con.Query("select mod(a, 40) from modme");
	REQUIRE(CHECK_COLUMN(result, 0, {2.123456}));

	// Mod with 0 should be null
	result = con.Query("select mod(42, 0)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// input is real, divisor is a real
	result = con.Query("select mod(a, 2) from modme");
	REQUIRE(CHECK_COLUMN(result, 0, {.123456}));

	// input is an integer, divisor is a real
	result = con.Query("select mod(b, 2.1) from modme");
	REQUIRE(CHECK_COLUMN(result, 0, {0.9}));
}
TEST_CASE("Power test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE powerme(a DOUBLE, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO powerme VALUES (2.1, 3)"));

	result = con.Query("select pow(a, 0) from powerme");
	REQUIRE(CHECK_COLUMN(result, 0, {1.0}));

	result = con.Query("select pow(b, -2) from powerme");
	REQUIRE(CHECK_COLUMN(result, 0, {.1111}));

	result = con.Query("select pow(a, b) from powerme");
	REQUIRE(CHECK_COLUMN(result, 0, {9.261}));

	result = con.Query("select pow(b, a) from powerme");
	REQUIRE(CHECK_COLUMN(result, 0, {10.045}));

	result = con.Query("select power(b, a) from powerme");
	REQUIRE(CHECK_COLUMN(result, 0, {10.045}));
}

TEST_CASE("BIT_COUNT test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bits(t tinyint, s smallint, i integer, b bigint)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bits VALUES (NULL, NULL, NULL, NULL), "
	                                                  "(31, 1023, 11834119, 50827156903621017), "
	                                                  "(-59, -517, -575693, -9876543210)"));

	result = con.Query("select bit_count(t), bit_count(s),bit_count(i), bit_count(b) from bits");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(),  5,  4}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 10, 14}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 11, 24}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), 27, 49}));
}

TEST_CASE("Test invalid input for math functions", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// any invalid input in math functions results in a NULL
	// sqrt of negative number
	result = con.Query("SELECT SQRT(-1), SQRT(0)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));

	// log of value <= 0
	result = con.Query("SELECT LN(-1), LN(0), LOG10(-1), LOG10(0), LOG2(-1), LOG2(0)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value()}));

	// invalid input to POW function
	result = con.Query("SELECT POW(1e300,100), POW(-1e300,100), POW(-1.0, 0.5)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));

	// overflow in EXP function
	result = con.Query("SELECT EXP(1e300), EXP(1e100)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	// invalid input to trigonometric functions
	result = con.Query("SELECT ACOS(3), ACOS(100), DEGREES(1e308)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
}
