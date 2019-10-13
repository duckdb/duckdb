#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Extract function", "[date]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dates(i DATE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dates VALUES ('1993-08-14'), (NULL)"));

	// extract various parts of the date
	// year
	result = con.Query("SELECT EXTRACT(year FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(1993), Value()}));
	// month
	result = con.Query("SELECT EXTRACT(month FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(8), Value()}));
	// day
	result = con.Query("SELECT EXTRACT(day FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(14), Value()}));
	// decade
	result = con.Query("SELECT EXTRACT(decade FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(199), Value()}));
	// century
	result = con.Query("SELECT EXTRACT(century FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(20), Value()}));
	// day of the week (Sunday = 0, Saturday = 6)
	result = con.Query("SELECT EXTRACT(DOW FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(6), Value()}));
	// day of the year (1 - 365/366)
	result = con.Query("SELECT EXTRACT(DOY FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(226), Value()}));
	// epoch
	result = con.Query("SELECT EXTRACT(epoch FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(745286400), Value()}));
	// isodow (Monday = 1, Sunday = 7)
	result = con.Query("SELECT EXTRACT(ISODOW FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(6), Value()}));
	// millenium (change of millenium is January 1, X001)
	result = con.Query("SELECT EXTRACT(millennium FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(2), Value()}));
	// timestamp variants all give 0 for date
	result = con.Query("SELECT EXTRACT(second FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(0), Value()}));
	result = con.Query("SELECT EXTRACT(minute FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(0), Value()}));
	result = con.Query("SELECT EXTRACT(hour FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(0), Value()}));
	result = con.Query("SELECT EXTRACT(milliseconds FROM i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(0), Value()}));
}

TEST_CASE("Extract function edge cases", "[date]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// century changes in the year 1
	result = con.Query("SELECT EXTRACT(century FROM cast('2000-10-10' AS DATE));");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(20)}));
	result = con.Query("SELECT EXTRACT(century FROM cast('2001-10-10' AS DATE));");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(21)}));
	// millennium changes in the year 1
	result = con.Query("SELECT EXTRACT(millennium FROM cast('2000-10-10' AS DATE));");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(2)}));
	result = con.Query("SELECT EXTRACT(millennium FROM cast('2001-10-10' AS DATE));");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(3)}));
	// check DOW
	// start from the epoch and go up/down, every time the day should go up/down
	// one as well
	int epoch_day = 4;
	int expected_day_up = epoch_day, expected_day_down = epoch_day;
	for (size_t i = 0; i < 7; i++) {
		result = con.Query("SELECT EXTRACT(dow FROM cast('1970-01-01' AS DATE) + " + to_string(i) + ");");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(expected_day_up)}));
		result = con.Query("SELECT EXTRACT(dow FROM cast('1970-01-01' AS DATE) - " + to_string(i) + ");");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(expected_day_down)}));
		expected_day_up = (expected_day_up + 1) % 7;
		expected_day_down = expected_day_down == 0 ? 6 : expected_day_down - 1;
	}

	// week numbers are weird
	result = con.Query("SELECT EXTRACT(week FROM cast('2005-01-01' AS DATE));");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(53)}));
	result = con.Query("SELECT EXTRACT(week FROM cast('2006-01-01' AS DATE));");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(52)}));
	result = con.Query("SELECT EXTRACT(week FROM cast('2007-01-01' AS DATE));");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(1)}));
	result = con.Query("SELECT EXTRACT(week FROM cast('2008-01-01' AS DATE));");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(1)}));
	result = con.Query("SELECT EXTRACT(week FROM cast('2009-01-01' AS DATE));");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(1)}));
	result = con.Query("SELECT EXTRACT(week FROM cast('2010-01-01' AS DATE));");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(53)}));

	// every 7 days the week number should go up by 7
	int expected_week = 1;
	for (size_t i = 0; i < 40; i++) {
		result = con.Query("SELECT EXTRACT(week FROM cast('2007-01-01' AS DATE) + " + to_string(i * 7) + ");");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(expected_week)}));
		expected_week++;
	}
}

TEST_CASE("Extract timestamp function", "[timestamp]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE timestamps(i TIMESTAMP)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamps VALUES ('1993-08-14 08:22:33'), (NULL)"));

	// extract various parts of the date
	// year
	result = con.Query("SELECT EXTRACT(year FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(1993), Value()}));
	// month
	result = con.Query("SELECT EXTRACT(month FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(8), Value()}));
	// day
	result = con.Query("SELECT EXTRACT(day FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(14), Value()}));
	// decade
	result = con.Query("SELECT EXTRACT(decade FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(199), Value()}));
	// century
	result = con.Query("SELECT EXTRACT(century FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(20), Value()}));
	// day of the week (Sunday = 0, Saturday = 6)
	result = con.Query("SELECT EXTRACT(DOW FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(6), Value()}));
	// day of the year (1 - 365/366)
	result = con.Query("SELECT EXTRACT(DOY FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(226), Value()}));
	// epoch
	result = con.Query("SELECT EXTRACT(epoch FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(745316553), Value()}));
	// isodow (Monday = 1, Sunday = 7)
	result = con.Query("SELECT EXTRACT(ISODOW FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(6), Value()}));
	// millenium (change of millenium is January 1, X001)
	result = con.Query("SELECT EXTRACT(millennium FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(2), Value()}));
	result = con.Query("SELECT EXTRACT(second FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(33), Value()}));
	result = con.Query("SELECT EXTRACT(minute FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(22), Value()}));
	result = con.Query("SELECT EXTRACT(hour FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(8), Value()}));
	result = con.Query("SELECT EXTRACT(milliseconds FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(33000), Value()}));
}

TEST_CASE("Extract milliseconds from timestamp", "[timestamp]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE timestamps(i TIMESTAMP)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamps VALUES ('1993-08-14 08:22:33.42'), (NULL)"));

	result = con.Query("SELECT EXTRACT(second FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DOUBLE(33), Value()})); // postgres returns 33.42 here
	result = con.Query("SELECT EXTRACT(minute FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(22), Value()}));
	result = con.Query("SELECT EXTRACT(milliseconds FROM i) FROM timestamps");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(33420), Value()}));
}
