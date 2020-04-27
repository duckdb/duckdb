#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar printf", "[printf]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// printf without format specifiers
	result = con.Query("SELECT printf('hello'), printf(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	// format strings
	result = con.Query("SELECT printf('%s', 'hello'), printf('%s: %s', 'hello', 'world')");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello: world"}));

	// format strings with NULL values
	result = con.Query("SELECT printf('%s', NULL), printf(NULL, 'hello', 'world')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	// booleans
	result = con.Query("SELECT printf('%d', TRUE)");
	REQUIRE(CHECK_COLUMN(result, 0, {"1"}));

	// integers
	result = con.Query("SELECT printf('%d', 33), printf('%d + %d = %d', 3, 5, 3 + 5)");
	REQUIRE(CHECK_COLUMN(result, 0, {"33"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"3 + 5 = 8"}));

	// integers with special formatting specifiers
	result = con.Query(
	    "SELECT printf('%04d', 33), printf('%s %02d:%02d:%02d %s', 'time', 12, 3, 16, 'AM'), printf('%10d', 1992)");
	REQUIRE(CHECK_COLUMN(result, 0, {"0033"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"time 12:03:16 AM"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"      1992"}));

	// different integer types
	result = con.Query("SELECT printf('%hhd %hd %d %lld', 33::TINYINT, 12::SMALLINT, 40::INTEGER, 80::BIGINT)");
	REQUIRE(CHECK_COLUMN(result, 0, {"33 12 40 80"}));
	// ...but really any of these can be used
	result = con.Query("SELECT printf('%d %lld %hhd %hd', 33::TINYINT, 12::SMALLINT, 40::INTEGER, 80::BIGINT)");
	REQUIRE(CHECK_COLUMN(result, 0, {"33 12 40 80"}));

	// octal hex etc
	result = con.Query("SELECT printf('%d %x %o %#x %#o', 100, 100, 100, 100, 100)");
	REQUIRE(CHECK_COLUMN(result, 0, {"100 64 144 0x64 0144"}));

	// ascii characters
	result = con.Query("SELECT printf('%c', 65)");
	REQUIRE(CHECK_COLUMN(result, 0, {"A"}));

	// width trick
	result = con.Query("SELECT printf('%*d', 5, 10)");
	REQUIRE(CHECK_COLUMN(result, 0, {"   10"}));

	// floating point numbers
	result = con.Query("SELECT printf('%.2f', 10.0::FLOAT), printf('%.4f', 0.5)");
	REQUIRE(CHECK_COLUMN(result, 0, {"10.00"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"0.5000"}));

	// weird float stuff
	result = con.Query("SELECT printf('floats: %4.2f %+.0e %E', 3.1416, 3.1416, 3.1416)");
	REQUIRE(CHECK_COLUMN(result, 0, {"floats: 3.14 +3e+00 3.141600E+00"}));

	// incorrect number of parameters
	// too few parameters
	REQUIRE_FAIL(con.Query("SELECT printf('%s')"));
	REQUIRE_FAIL(con.Query("SELECT printf('%s %s', 'hello')"));
	// excess parameters are ignored
	result = con.Query("SELECT printf('%s', 'hello', 'world')");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	// incorrect types
	REQUIRE_FAIL(con.Query("SELECT printf('%s', 42)"));
	REQUIRE_FAIL(con.Query("SELECT printf('%d', 'hello')"));
}

TEST_CASE("Test printf with vectors", "[printf]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(idx INTEGER, fmt STRING, pint INTEGER, pstring STRING)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES (1, '%d: %s', 10, 'hello')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES (2, 'blabla %d blabla %s', 20, 'blabla')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES (3, NULL, 30, 'abcde')"));

	// printf without format specifiers: too few parameters
	REQUIRE_FAIL(con.Query("SELECT printf(fmt) FROM strings ORDER BY idx"));

	result = con.Query("SELECT printf(CASE WHEN pint < 15 THEN NULL ELSE pint END) FROM strings ORDER BY idx");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "20", "30"}));

	// standard vectorized printf
	result = con.Query("SELECT printf(fmt, pint, pstring) FROM strings ORDER BY idx");
	REQUIRE(CHECK_COLUMN(result, 0, {"10: hello", "blabla 20 blabla blabla", Value()}));

	// printf with constants in format arguments
	result = con.Query("SELECT printf(fmt, 10, pstring) FROM strings ORDER BY idx");
	REQUIRE(CHECK_COLUMN(result, 0, {"10: hello", "blabla 10 blabla blabla", Value()}));

	// printf with constant format string
	result = con.Query("SELECT printf('%s: %s', pstring, pstring) FROM strings ORDER BY idx");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello: hello", "blabla: blabla", "abcde: abcde"}));

	// printf with selection vector
	result = con.Query("SELECT printf('%s: %s', pstring, pstring) FROM strings WHERE idx <> 2 ORDER BY idx");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello: hello", "abcde: abcde"}));
}

TEST_CASE("Test scalar format", "[printf]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// format without format specifiers
	result = con.Query("SELECT format('hello'), format(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	// format strings
	result = con.Query("SELECT format('{}', 'hello'), format('{}: {}', 'hello', 'world')");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello: world"}));

	// format strings with NULL values
	result = con.Query("SELECT format('{}', NULL), format(NULL, 'hello', 'world')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	// booleans
	result = con.Query("SELECT format('{} {}', TRUE, FALSE)");
	REQUIRE(CHECK_COLUMN(result, 0, {"true false"}));

	// integers
	result = con.Query("SELECT format('{}', 33), format('{} + {} = {}', 3, 5, 3 + 5)");
	REQUIRE(CHECK_COLUMN(result, 0, {"33"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"3 + 5 = 8"}));

	// integers with special formatting specifiers
	result = con.Query("SELECT format('{:04d}', 33), format('{} {:02d}:{:02d}:{:02d} {}', 'time', 12, 3, 16, 'AM'), "
	                   "format('{:10d}', 1992)");
	REQUIRE(CHECK_COLUMN(result, 0, {"0033"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"time 12:03:16 AM"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"      1992"}));

	// numeric input of arguments
	result = con.Query("SELECT format('{1} {1} {0} {0}', 1, 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {"2 2 1 1"}));

	// incorrect number of parameters
	// too few parameters
	REQUIRE_FAIL(con.Query("SELECT format('{}')"));
	REQUIRE_FAIL(con.Query("SELECT format('{} {}', 'hello')"));
	// excess parameters are ignored
	result = con.Query("SELECT format('{}', 'hello', 'world')");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	// incorrect types
	REQUIRE_FAIL(con.Query("SELECT format('{:s}', 42)"));
	REQUIRE_FAIL(con.Query("SELECT format('{:d}', 'hello')"));
}
