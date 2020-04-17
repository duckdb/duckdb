#include "catch.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "test_helpers.hpp"
#include "iostream"

using namespace duckdb;
using namespace std;

TEST_CASE("Test date truncate functionality", "[date]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dates(d DATE, s VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE timestamps(d TIMESTAMP, s VARCHAR);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO dates VALUES ('1992-12-02', 'year'), ('1993-03-03', 'month'), ('1994-05-05', 'day');"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamps VALUES "
	                          "('1992-02-02 02:02:03', 'millennium'), "
	                          "('1992-02-02 02:02:03', 'century'), "
	                          "('1992-02-02 02:02:03', 'decade'), "
	                          "('1992-02-02 02:02:03', 'year'), "
	                          "('1992-02-02 02:02:03', 'quarter'), "
	                          "('1992-02-02 02:02:03', 'month'), "
	                          "('1992-02-02 02:02:03', 'week'), "
	                          "('1992-02-02 02:02:03', 'day'), "
	                          "('1992-02-02 02:02:03', 'hour'), "
	                          "('1992-02-02 02:02:03', 'minute'), "
	                          "('1992-02-02 02:02:03', 'second'), "
	                          "('1992-02-02 02:02:03', 'milliseconds'), "
	                          "('1992-02-02 02:02:03', 'microseconds');"));

	// test date_trunc with different combinations of constant/non-constant columns on both dates and timestamps
	result = con.Query("SELECT date_trunc(NULL::VARCHAR, NULL::TIMESTAMP) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	result = con.Query("SELECT date_trunc(s, NULL::TIMESTAMP) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	result = con.Query("SELECT date_trunc(NULL, d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	result = con.Query("SELECT date_trunc(NULL::VARCHAR, NULL::TIMESTAMP) FROM timestamps LIMIT 3;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	result = con.Query("SELECT date_trunc(s, NULL::TIMESTAMP) FROM timestamps LIMIT 3;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));
	result = con.Query("SELECT date_trunc(NULL, d) FROM timestamps LIMIT 3;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), Value()}));

	// dates should be cast to timestamp correctly
	result = con.Query("SELECT date_trunc('month', DATE '1992-02-02') FROM dates LIMIT 1;");
	REQUIRE(result->sql_types[0] == SQLType::TIMESTAMP);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("1992-02-01 00:00:00"))}));
	result = con.Query("SELECT date_trunc(s, d) FROM dates;");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::BIGINT(Timestamp::FromString("1992-01-01 00:00:00")),
	                      Value::BIGINT(Timestamp::FromString("1993-03-01 00:00:00")),
	                      Value::BIGINT(Timestamp::FromString("1994-05-05 00:00:00"))}));

	// Timestamps should return timestamp type
	result = con.Query("SELECT date_trunc('minute', TIMESTAMP '1992-02-02 04:03:02') FROM timestamps LIMIT 1;");
	REQUIRE(result->sql_types[0] == SQLType::TIMESTAMP);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("1992-02-02 04:03:00"))}));

	// Test all truncate operators on timestamps
	result = con.Query("SELECT date_trunc(s, d) FROM timestamps;");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {
	                         Value::BIGINT(Timestamp::FromString("1000-01-01 00:00:00")), // millennium
	                         Value::BIGINT(Timestamp::FromString("1900-01-01 00:00:00")), // century
	                         Value::BIGINT(Timestamp::FromString("1990-01-01 00:00:00")), // decade
	                         Value::BIGINT(Timestamp::FromString("1992-01-01 00:00:00")), // year
	                         Value::BIGINT(Timestamp::FromString("1992-01-01 00:00:00")), // quarter
	                         Value::BIGINT(Timestamp::FromString("1992-02-01 00:00:00")), // month
	                         Value::BIGINT(Timestamp::FromString("1992-01-27 00:00:00")), // week
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 00:00:00")), // day
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 02:00:00")), // hour
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 02:02:00")), // minute
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 02:02:03")), // second
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 02:02:03")), // millisecond
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 02:02:03"))  // microsecond
	                     }));

	// Redo previous test but with casting to date first
	result = con.Query("SELECT date_trunc(s, CAST(d as DATE)) FROM timestamps;");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {
	                         Value::BIGINT(Timestamp::FromString("1000-01-01 00:00:00")), // millennium
	                         Value::BIGINT(Timestamp::FromString("1900-01-01 00:00:00")), // century
	                         Value::BIGINT(Timestamp::FromString("1990-01-01 00:00:00")), // decade
	                         Value::BIGINT(Timestamp::FromString("1992-01-01 00:00:00")), // year
	                         Value::BIGINT(Timestamp::FromString("1992-01-01 00:00:00")), // quarter
	                         Value::BIGINT(Timestamp::FromString("1992-02-01 00:00:00")), // month
	                         Value::BIGINT(Timestamp::FromString("1992-01-27 00:00:00")), // week
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 00:00:00")), // day
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 00:00:00")), // hour
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 00:00:00")), // minute
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 00:00:00")), // second
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 00:00:00")), // millisecond
	                         Value::BIGINT(Timestamp::FromString("1992-02-02 00:00:00"))  // microsecond
	                     }));

	// Test week operator special cases
	result = con.Query("SELECT date_trunc('week', TIMESTAMP '2020-01-01 04:03:02') FROM timestamps LIMIT 1;");
	REQUIRE(result->sql_types[0] == SQLType::TIMESTAMP);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2019-12-30 00:00:00"))}));
	result = con.Query("SELECT date_trunc('week', TIMESTAMP '2019-01-06 04:03:02') FROM timestamps LIMIT 1;");
	REQUIRE(result->sql_types[0] == SQLType::TIMESTAMP);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2018-12-31 00:00:00"))}));

	// Test quarter operator more thoroughly
	result = con.Query("SELECT date_trunc('quarter', TIMESTAMP '2020-12-02 04:03:02') FROM timestamps LIMIT 1;");
	REQUIRE(result->sql_types[0] == SQLType::TIMESTAMP);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2020-10-01 00:00:00"))}));
	result = con.Query("SELECT date_trunc('quarter', TIMESTAMP '2019-01-06 04:03:02') FROM timestamps LIMIT 1;");
	REQUIRE(result->sql_types[0] == SQLType::TIMESTAMP);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2019-01-01 00:00:00"))}));

	// Unknown specifier should fail
	REQUIRE_FAIL(con.Query("SELECT date_trunc('epoch', TIMESTAMP '2019-01-06 04:03:02') FROM timestamps LIMIT 1;"));
}
