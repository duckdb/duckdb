#include "catch.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

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

	// we can add together intervals
	// result = con.Query("SELECT INTERVAL '2 month' + INTERVAL '1 month 3 days';");
	// REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(3, 3, 0)}));
	// // or subtract them
	// result = con.Query("SELECT INTERVAL '2 month' - INTERVAL '1 month 3 days';");
	// REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(1, -3, 0)}));
	// but not multiply
	// REQUIRE_FAIL(con.Query("SELECT INTERVAL '2 month' * INTERVAL '1 month 3 days';"));

	// we can add them to dates...
	// we can add them to times...
	// we can add them to timestamps...

	// invalid intervals
	// empty interval
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '   	';"));
	// no number
	REQUIRE_FAIL(con.Query("SELECT INTERVAL 'years';"));
	REQUIRE_FAIL(con.Query("SELECT INTERVAL '-years';"));
	// FIXME: overflows
	// what if year + year overflows?
	// overflow in year
	// REQUIRE_FAIL(con.Query("SELECT INTERVAL '100000000000000000year';"));
	// // overflow in months
	// REQUIRE_FAIL(con.Query("SELECT INTERVAL '100000000000000000months';"));
	// // overflow in days
	// REQUIRE_FAIL(con.Query("SELECT INTERVAL '100000000000000000days';"));


}

