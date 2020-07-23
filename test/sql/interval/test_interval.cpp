#include "catch.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/interval.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test basic interval usage", "[interval]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// 2 years
	result = con.Query("SELECT INTERVAL '2 years'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(24, 0, 0)}));
	result = con.Query("SELECT INTERVAL '2 years'::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {"2 years"}));
	// 2 years one minute
	result = con.Query("SELECT INTERVAL '2Y 1 M';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(24, 0, 60*1000)}));
	// 2 years 4 days one minute 3 seconds 20 milliseconds
	result = con.Query("SELECT INTERVAL '2Y 1 month 1 M 3S 20mS';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(25, 0, 63*1000 + 20)}));
	result = con.Query("SELECT INTERVAL '2Y 1 month 1M 3S 20mS'::VARCHAR;");
	REQUIRE(CHECK_COLUMN(result, 0, {"2 years 1 month 00:01:03.020"}));
	// -2 years +4 days +one minute 3 seconds 20 milliseconds
	result = con.Query("SELECT INTERVAL '-2Y 4 days 1 MinUteS 3S 20mS';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(-24, 4, 63*1000 + 20)}));
	result = con.Query("SELECT INTERVAL '-2Y 4 days 1 MinUteS 3S 20mS'::VARCHAR;");
	REQUIRE(CHECK_COLUMN(result, 0, {"-2 years 4 days 00:01:03.020"}));
	// test ago usage
	result = con.Query("SELECT INTERVAL '2Y 4 days 1 MinUteS 3S 20mS ago'::VARCHAR;");
	REQUIRE(CHECK_COLUMN(result, 0, {"-2 years -4 days -00:01:03.020"}));
	// months and hours, with optional @
	result = con.Query("SELECT INTERVAL '@2mons 1H';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(2, 0, 60 * 60 * 1000)}));
	// FIXME
	// we can also use the ISO 8601 interval format
	// result = con.Query("SELECT INTERVAL 'P2MT1H1M';");
	// REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(2, 0, 60 * 60 * 1000 + 60 * 1000)}));
	// or this format
	// result = con.Query("SELECT INTERVAL 'P00-02-00T01:00:01';");
	// REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(2, 0, 60 * 60 * 1000 + 60 * 1000)}));

	result = con.Query("SELECT INTERVAL '90' DAY;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 90, 0)}));
	result = con.Query("SELECT INTERVAL '90' YEAR;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(12 * 90, 0, 0)}));
	result = con.Query("SELECT INTERVAL '1' YEAR TO MONTH;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(12, 0, 0)}));
	result = con.Query("SELECT INTERVAL '90' MONTH;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(90, 0, 0)}));
	result = con.Query("SELECT INTERVAL '90' SECOND;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 0, 90 * 1000)}));
	result = con.Query("SELECT INTERVAL '90' MINUTE;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 0, 90 * 60 * 1000)}));
	result = con.Query("SELECT INTERVAL '90' MINUTE TO SECOND;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 0, 90 * 60 * 1000)}));
	result = con.Query("SELECT INTERVAL '90' HOUR;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 0, 90 * 60 * 60 * 1000)}));
	result = con.Query("SELECT INTERVAL '90' HOUR TO MINUTE;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 0, 90 * 60 * 60 * 1000)}));
	result = con.Query("SELECT INTERVAL '90' HOUR TO SECOND;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 0, 90 * 60 * 60 * 1000)}));
	result = con.Query("SELECT INTERVAL '1' DAY TO HOUR;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 0, 24 * 60 * 60 * 1000)}));
	result = con.Query("SELECT INTERVAL '1' DAY TO MINUTE;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 0, 24 * 60 * 60 * 1000)}));
	result = con.Query("SELECT INTERVAL '1' DAY TO SECOND;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 0, 24 * 60 * 60 * 1000)}));

	// we can add together intervals
	result = con.Query("SELECT INTERVAL '2 month' + INTERVAL '1 month 3 days';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(3, 3, 0)}));
	// or subtract them
	result = con.Query("SELECT INTERVAL '2 month' - INTERVAL '1 month 3 days';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(1, -3, 0)}));
	// but not multiply
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '2 month' * INTERVAL '1 month 3 days';"));

	// we can, however, multiply/divide intervals by integers
	result = con.Query("SELECT INTERVAL '1 year 2 days 2 seconds' * 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(24, 4, 4 * 1000)}));
	// multiplication can be done both ways
	result = con.Query("SELECT 2 * INTERVAL '1 year 2 days 2 seconds';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(24, 4, 4 * 1000)}));
	result = con.Query("SELECT INTERVAL '1 year 2 days 2 seconds' / 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(6, 1, 1 * 1000)}));
	// division cannot!
	REQUIRE_FAIL(con.Query("SELECT 2 / INTERVAL '1 year 2 days 2 seconds';"));
	// division by zero
	result = con.Query("SELECT INTERVAL '1 year 2 days 2 seconds' / 0;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// invalid intervals
	// empty interval
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '   	';"));
	// no number
	REQUIRE_FAIL(con.Query("SELECT INTERVAL 'years';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '-years';"));
	// gibberish
	REQUIRE_FAIL(con.Query("SELECT INTERVAL 'aergjaerghiuaehrgiuhaerg';"));

	// overflow in year
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '100000000000000000year';"));
	// overflow in months
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '100000000000000000months';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '4294967296months';"));
	REQUIRE_NO_FAIL(con.Query("SELECT INTERVAL '1294967296months';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '1294967296months 1294967296months';"));
	REQUIRE_NO_FAIL(con.Query("SELECT INTERVAL '1294967296months -1294967296months';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '-1294967296months -1294967296months';"));
	// overflow in days
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '100000000000000000days';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '1294967296days 1294967296days';"));
	// overflow in msecs
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '100000000000000000000msecs';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '100000000000000000hours';"));
	REQUIRE_NO_FAIL(con.Query("SELECT INTERVAL '2562047788000 hours';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '2562047788000 hours 2562047788000 hours';"));
	REQUIRE_NO_FAIL(con.Query("SELECT INTERVAL '-9223372036854775807msecs';"));
	REQUIRE_NO_FAIL(con.Query("SELECT INTERVAL '9223372036854775807msecs';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '9223372036854775810msecs';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '-9223372036854775810msecs';"));
	// need a number here
	REQUIRE_FAIL(con.Query("SELECT INTERVAL 'aa' DAY;"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '100 months' DAY;"));
}


TEST_CASE("Test interval addition/subtraction", "[interval]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// we can add/subtract intervals to/from dates
	result = con.Query("SELECT DATE '1992-03-01' + INTERVAL '1' YEAR");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1993, 3, 1)}));
	// check a bunch of different months to test proper month looping behavior
	for(int i = 0; i <= 12; i++) {
		result = con.Query("SELECT DATE '1992-03-01' + INTERVAL '" + to_string(i) + "' MONTH");
		if (i + 3 <= 12) {
			REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1992, 3 + i, 1)}));
		} else {
			REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1993, i - (12 - 3), 1)}));
		}
		result = con.Query("SELECT DATE '1992-03-01' - INTERVAL '" + to_string(i) + "' MONTH");
		if (3 - i >= 1) {
			REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1992, 3 - i, 1)}));
		} else {
			REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1991, 12 - (i - 3), 1)}));
		}
	}
	result = con.Query("SELECT DATE '1992-03-01' + INTERVAL '10' DAY");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1992, 3, 11)}));
	result = con.Query("SELECT DATE '1992-03-01' - INTERVAL '10' DAY");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1992, 2, 20)}));
	result = con.Query("SELECT DATE '1993-03-01' - INTERVAL '10' DAY");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1993, 2, 19)}));
	// small times have no impact on date
	result = con.Query("SELECT DATE '1993-03-01' - INTERVAL '1' SECOND");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1993, 3, 1)}));
	// small seconds have no impact on DATE
	result = con.Query("SELECT DATE '1993-03-01' + INTERVAL '1' SECOND");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1993, 3, 1)}));
	result = con.Query("SELECT DATE '1993-03-01' - INTERVAL '1' SECOND");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1993, 3, 1)}));
	// but a large amount of seconds does have an impact
	result = con.Query("SELECT DATE '1993-03-01' + INTERVAL '1000000' SECOND");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1993, 3, 12)}));
	result = con.Query("SELECT DATE '1993-03-01' - INTERVAL '1000000' SECOND");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1993, 2, 18)}));
	// we cannot subtract dates from intervals
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '1000000' SECOND - DATE '1993-03-01'"));

	// we can add/subtract them to/from times
	result = con.Query("SELECT TIME '10:00:00' + INTERVAL '5' SECOND");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(10, 0, 5, 0)}));
	result = con.Query("SELECT INTERVAL '5' SECOND + TIME '10:00:00'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(10, 0, 5, 0)}));
	result = con.Query("SELECT TIME '10:00:00' - INTERVAL '5' SECOND");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(9, 59, 55, 0)}));
	// adding large amounts does nothing
	result = con.Query("SELECT TIME '10:00:00' + INTERVAL '1' DAY");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(10, 0, 0, 0)}));
	result = con.Query("SELECT TIME '10:00:00' + INTERVAL '1' DAY TO HOUR");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(10, 0, 0, 0)}));
	result = con.Query("SELECT TIME '10:00:00' - INTERVAL '1' DAY TO HOUR");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(10, 0, 0, 0)}));
	// test wrapping behavior
	result = con.Query("SELECT TIME '23:00:00' + INTERVAL '1' HOUR");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(0, 0, 0, 0)}));
	result = con.Query("SELECT TIME '00:00:00' - INTERVAL '1' HOUR");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(23, 0, 0, 0)}));
	result = con.Query("SELECT TIME '00:00:00' + INTERVAL '-1' HOUR");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(23, 0, 0, 0)}));

	// we can add/subtract them to/from timestamps
	result = con.Query("SELECT TIMESTAMP '1992-01-01 10:00:00' + INTERVAL '1' DAY");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIMESTAMP(1992, 1, 2, 10, 0, 0, 0)}));
	result = con.Query("SELECT INTERVAL '1' DAY + TIMESTAMP '1992-01-01 10:00:00'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIMESTAMP(1992, 1, 2, 10, 0, 0, 0)}));
	result = con.Query("SELECT TIMESTAMP '1992-01-01 10:00:05' + INTERVAL '17 years 3 months 1 day 2 hours 1 minute 57 seconds'");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIMESTAMP(2009, 4, 2, 12, 2, 2, 0)}));
	result = con.Query("SELECT TIMESTAMP '1992-01-01 10:00:00' - INTERVAL '1' DAY");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIMESTAMP(1991, 12, 31, 10, 0, 0, 0)}));
	result = con.Query("select timestamp '1993-01-01 00:00:00' - timestamp '1991-01-01 01:00:30';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(23, 30, 22 * Interval::MSECS_PER_HOUR + 59 * Interval::MSECS_PER_MINUTE + 30 * Interval::MSECS_PER_SEC)}));
}

TEST_CASE("Test storage for interval type", "[interval]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_interval_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE interval (t INTERVAL);"));
		REQUIRE_NO_FAIL(con.Query(
		    "INSERT INTO interval VALUES (INTERVAL '1' DAY), (NULL), (INTERVAL '3 months 2 days 5 seconds')"));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT t FROM interval ORDER BY t;");
		REQUIRE(CHECK_COLUMN(result, 0,
		                     {Value(), Value::INTERVAL(0, 1, 0), Value::INTERVAL(3, 2, 5000)}));
		result = con.Query("SELECT t FROM interval WHERE t = INTERVAL '1' DAY;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 1, 0)}));
		result = con.Query("SELECT t FROM interval WHERE t >= INTERVAL '1' DAY ORDER BY 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 1, 0), Value::INTERVAL(3, 2, 5000)}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test interval comparisons", "[interval]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// 30 days = 1 month for ordering purposes, but NOT for equality purposes
	result = con.Query("SELECT INTERVAL '30' DAY > INTERVAL '1' MONTH");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT INTERVAL '30' DAY = INTERVAL '1' MONTH");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT INTERVAL '30' DAY >= INTERVAL '1' MONTH");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT INTERVAL '31' DAY > INTERVAL '1' MONTH");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT INTERVAL '2' DAY TO HOUR > INTERVAL '1' DAY");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT INTERVAL '1' DAY TO HOUR >= INTERVAL '1' DAY");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	result = con.Query("SELECT INTERVAL '1' HOUR < INTERVAL '1' DAY");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT INTERVAL '30' HOUR <= INTERVAL '1' DAY");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	result = con.Query("SELECT INTERVAL '1' HOUR = INTERVAL '1' HOUR");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	result = con.Query("SELECT INTERVAL '1' YEAR = INTERVAL '12' MONTH");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
}

TEST_CASE("Test various ops involving intervals", "[interval]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);
	con.EnableQueryVerification();

	// create table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE interval (t INTERVAL);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO interval VALUES (INTERVAL '20' DAY), (INTERVAL '1' YEAR), (INTERVAL '1' MONTH);"));

	// count distinct
	result = con.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// update
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("UPDATE interval SET t=INTERVAL '1' MONTH WHERE t=INTERVAL '20' DAY;"));
	// now we only have two distinct values in con
	result = con.Query("SELECT * FROM interval ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(12, 0, 0)}));
	result = con.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	// in con2 we still have 3
	result = con2.Query("SELECT * FROM interval ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 20, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(12, 0, 0)}));
	result = con2.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	// rollback
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// after the rollback we are back to 3
	result = con.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// now commit it
	REQUIRE_NO_FAIL(con.Query("UPDATE interval SET t=INTERVAL '1' MONTH WHERE t=INTERVAL '20' DAY;"));
	result = con.Query("SELECT t, COUNT(*) FROM interval GROUP BY t ORDER BY 2 DESC");
	REQUIRE(CHECK_COLUMN(result, 1, {2, 1}));
	result = con.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con2.Query("SELECT COUNT(DISTINCT t) FROM interval");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT * FROM interval i1 JOIN interval i2 USING (t) ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(12, 0, 0)}));
	result = con.Query("SELECT * FROM interval i1 JOIN interval i2 ON (i1.t <> i2.t) ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(12, 0, 0),  Value::INTERVAL(12, 0, 0)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::INTERVAL(12, 0, 0), Value::INTERVAL(12, 0, 0), Value::INTERVAL(1, 0, 0),  Value::INTERVAL(1, 0, 0)}));
	result = con.Query("SELECT * FROM interval i1 JOIN interval i2 ON (i1.t > i2.t) ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(12, 0, 0), Value::INTERVAL(12, 0, 0)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0)}));

	result = con.Query("SELECT t, row_number() OVER (PARTITION BY t ORDER BY t) FROM interval ORDER BY 1, 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(1, 0, 0), Value::INTERVAL(1, 0, 0), Value::INTERVAL(12, 0, 0)}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 1}));
}
