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

	//  aliases
	result = con.Query("SELECT DAYOFMONTH(d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3, 5}));
	result = con.Query("SELECT WEEKDAY(d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2, 2}));
}
