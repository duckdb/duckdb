#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test basic DATE functionality", "[date]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.EnableQueryVerification();

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dates(i DATE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dates VALUES ('1993-08-14'), (NULL)"));

	// check that we can select dates
	result = con.Query("SELECT * FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(Date::FromDate(1993, 8, 14)), Value()}));

	// YEAR function
	result = con.Query("SELECT year(i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(1993), Value()}));

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

	// FIXME: this
	// // we also can't add two dates together
	// REQUIRE_FAIL(con.Query("SELECT i + i FROM dates"));
	// // but we can subtract them! resulting in an integer
	// result = con.Query("SELECT (i + 5) - i FROM dates");
	// REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(5)}));
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
	// day out of range because not a leapyear
	REQUIRE_FAIL(con.Query("INSERT INTO dates VALUES ('1993-02-29')"));
	// day out of range because not a leapyear
	REQUIRE_FAIL(con.Query("INSERT INTO dates VALUES ('1900-02-29')"));
	// day in range because of leapyear
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dates VALUES ('1992-02-29')"));
	// day in range because of leapyear
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dates VALUES ('2000-02-29')"));
}
