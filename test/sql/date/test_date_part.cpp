#include "catch.hpp"
#include "duckdb/common/types/date.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("DATE_PART test", "[date]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dates(d DATE, s VARCHAR);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO dates VALUES ('1992-01-01', 'year'), ('1992-03-03', 'month'), ('1992-05-05', 'day');"));

	// test date_part with different combinations of constant/non-constant columns
	result = con.Query("SELECT date_part(NULL::VARCHAR, NULL::TIMESTAMP) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	result = con.Query("SELECT date_part(s, NULL::TIMESTAMP) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));

	// dates
	result = con.Query("SELECT date_part(NULL, d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	result = con.Query("SELECT date_part(s, DATE '1992-01-01') FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {1992, 1, 1}));
	result = con.Query("SELECT date_part('year', d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {1992, 1992, 1992}));
	result = con.Query("SELECT date_part(s, d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {1992, 3, 5}));

	// timestamps
	result = con.Query("SELECT date_part(NULL, d::TIMESTAMP) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	result = con.Query("SELECT date_part(s, TIMESTAMP '1992-01-01') FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {1992, 1, 1}));
	result = con.Query("SELECT date_part('year', d::TIMESTAMP) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {1992, 1992, 1992}));
	result = con.Query("SELECT date_part(s, d::TIMESTAMP) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {1992, 3, 5}));

	//  last_day
	result = con.Query("SELECT LAST_DAY(DATE '1900-02-12'), LAST_DAY(DATE '1992-02-12'), LAST_DAY(DATE '2000-02-12');");
	REQUIRE(CHECK_COLUMN(result, 0, {Date::FromDate(1900, 2, 28)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Date::FromDate(1992, 2, 29)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Date::FromDate(2000, 2, 29)}));
	result = con.Query("SELECT LAST_DAY(d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Date::FromDate(1992, 1, 31), Date::FromDate(1992, 3, 31), Date::FromDate(1992, 5, 31)}));
	result = con.Query("SELECT LAST_DAY(d::timestamp) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Date::FromDate(1992, 1, 31), Date::FromDate(1992, 3, 31), Date::FromDate(1992, 5, 31)}));

	//  monthname
	result = con.Query("SELECT MONTHNAME(d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {"January", "March", "May"}));

	//  dayname
	result = con.Query("SELECT DAYNAME(d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {"Wednesday", "Tuesday", "Tuesday"}));

	//  yearweek
	result = con.Query("SELECT YEARWEEK(d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {199201, 199209, 199218}));

	//  aliases
	result = con.Query("SELECT DAYOFMONTH(d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3, 5}));
	result = con.Query("SELECT WEEKDAY(d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2, 2}));
	result = con.Query("SELECT WEEKOFYEAR(d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 9, 18}));
}
