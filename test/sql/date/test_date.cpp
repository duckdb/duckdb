#include "catch.hpp"
#include "duckdb/common/types/date.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test basic DATE functionality", "[date]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dates(i DATE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dates VALUES ('1993-08-14'), (NULL)"));

	// check that we can select dates
	result = con.Query("SELECT * FROM dates");
	REQUIRE(result->sql_types[0] == SQLType::DATE);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(Date::FromDate(1993, 8, 14)), Value()}));

	// YEAR function
	result = con.Query("SELECT year(i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(1993), Value()}));

	// check that we can convert dates to string
	result = con.Query("SELECT cast(i AS VARCHAR) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("1993-08-14"), Value()}));

	// check that we can add days to a date
	result = con.Query("SELECT i + 5 FROM dates");
	REQUIRE(result->success);
	REQUIRE(result->sql_types[0] == SQLType::DATE);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(Date::FromDate(1993, 8, 19)), Value()}));

	// check that we can subtract days from a date
	result = con.Query("SELECT i - 5 FROM dates");
	REQUIRE(result->success);
	REQUIRE(result->sql_types[0] == SQLType::DATE);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(Date::FromDate(1993, 8, 9)), Value()}));

	// HOWEVER, we can't divide or multiply or modulo
	REQUIRE_FAIL(con.Query("SELECT i * 3 FROM dates"));
	REQUIRE_FAIL(con.Query("SELECT i / 3 FROM dates"));
	REQUIRE_FAIL(con.Query("SELECT i % 3 FROM dates"));

	// we also can't add two dates together
	REQUIRE_FAIL(con.Query("SELECT i + i FROM dates"));
	// but we can subtract them! resulting in an integer
	result = con.Query("SELECT (i + 5) - i FROM dates");
	REQUIRE(result->success);
	REQUIRE(result->sql_types[0] == SQLType::INTEGER);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(5), Value()}));
}

TEST_CASE("Test BC dates", "[date]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dates(i DATE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dates VALUES ('-1993-08-14'), (NULL)"));

	// check that we can select dates
	result = con.Query("SELECT * FROM dates");
	REQUIRE(result->sql_types[0] == SQLType::DATE);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(Date::FromDate(-1993, 8, 14)), Value()}));

	// YEAR function
	result = con.Query("SELECT year(i) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(-1993), Value()}));

	// check that we can convert dates to string
	result = con.Query("SELECT cast(i AS VARCHAR) FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("1993-08-14 (BC)"), Value()}));
}

TEST_CASE("Test out of range/incorrect date formats", "[date]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

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

	// test incorrect date formats
	// dd-mm-YYYY
	REQUIRE_FAIL(con.Query("INSERT INTO dates VALUES ('02-02-1992')"));
	// different separators are not supported
	// REQUIRE_FAIL(con.Query("INSERT INTO dates VALUES ('1900/01/01')"));
	REQUIRE_FAIL(con.Query("INSERT INTO dates VALUES ('1900a01a01')"));
	// this should work though
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dates VALUES ('1900-1-1')"));

	// out of range dates
	REQUIRE_FAIL(con.Query("INSERT INTO dates VALUES ('-100000000-01-01')"));
	REQUIRE_FAIL(con.Query("INSERT INTO dates VALUES ('1000000000-01-01')"));
}
