
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test basic DATE functionality", "[date]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dates(i DATE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dates VALUES ('1993-08-14'), (NULL)"));

	// check that we can select dates
	result = con.Query("SELECT * FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(Date::FromDate(1993, 8, 14)), Value()}));

	// check that we can convert dates to string
	result = con.Query("SELECT cast(i AS VARCHAR) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("1993-08-14"), Value()}));

	// check that we can add days to a date
	result = con.Query("SELECT i + 5 FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(Date::FromDate(1993, 8, 19)), Value()}));

	// check that we can subtract days from a date
	result = con.Query("SELECT i - 5 FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(Date::FromDate(1993, 8, 9)), Value()}));

	// HOWEVER, we can't divide or multiply or modulo
	REQUIRE_FAIL(con.Query("SELECT i * 3 FROM dates"));
	REQUIRE_FAIL(con.Query("SELECT i / 3 FROM dates"));
	REQUIRE_FAIL(con.Query("SELECT i % 3 FROM dates"));
	// we also can't add two dates together
	REQUIRE_FAIL(con.Query("SELECT i + i FROM dates"));
	// but we can subtract them!
	result = con.Query("SELECT (i + 5) - i FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(5)}));
}

TEST_CASE("Extract function", "[date]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

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
	result = con.Query("SELECT EXTRACT(isodow FROM i) FROM dates");
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

	// FIXME: this needs to be updated in 982 years
	result = con.Query("SELECT EXTRACT(MILLENNIUM FROM NOW())");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(3), Value()}));
}

TEST_CASE("Test out of range/incorrect date formats", "[date]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dates(i DATE)"));
	REQUIRE_FAIL(con.Query("INSERT INTO dates VALUES ('blabla')"));
	// month out of range
	REQUIRE_FAIL(con.Query("INSERT INTO dates VALUES ('1993-20-14')"));
	// day out of range
	REQUIRE_FAIL(con.Query("INSERT INTO dates VALUES ('1993-08-99')"));
}
