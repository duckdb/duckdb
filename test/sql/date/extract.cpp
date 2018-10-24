
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Extract function", "[date]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dates(i DATE)"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO dates VALUES ('1993-08-14'), (NULL)"));

	// FIXME: not implemented yet
	return;
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
}
