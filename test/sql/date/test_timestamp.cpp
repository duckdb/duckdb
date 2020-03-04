#include "catch.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test TIMESTAMP type", "[timestamp]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// creates a timestamp table with a timestamp column and inserts a value
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE IF NOT EXISTS timestamp (t TIMESTAMP);"));
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO timestamp VALUES ('2008-01-01 00:00:01'), (NULL), ('2007-01-01 00:00:01'), ('2008-02-01 "
	    "00:00:01'), "
	    "('2008-01-02 00:00:01'), ('2008-01-01 10:00:00'), ('2008-01-01 00:10:00'), ('2008-01-01 00:00:10')"));

	// check if we can select timestamps
	result = con.Query("SELECT timestamp '2017-07-23 13:10:11';");
	REQUIRE(result->sql_types[0] == SQLType::TIMESTAMP);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2017-07-23 13:10:11"))}));
	// check order
	result = con.Query("SELECT t FROM timestamp ORDER BY t;");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value(), Value::BIGINT(Timestamp::FromString("2007-01-01 00:00:01")),
	                      Value::BIGINT(Timestamp::FromString("2008-01-01 00:00:01")),
	                      Value::BIGINT(Timestamp::FromString("2008-01-01 00:00:10")),
	                      Value::BIGINT(Timestamp::FromString("2008-01-01 00:10:00")),
	                      Value::BIGINT(Timestamp::FromString("2008-01-01 10:00:00")),
	                      Value::BIGINT(Timestamp::FromString("2008-01-02 00:00:01")),
	                      Value::BIGINT(Timestamp::FromString("2008-02-01 00:00:01"))}));

	result = con.Query("SELECT MIN(t) FROM timestamp;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2007-01-01 00:00:01"))}));

	result = con.Query("SELECT MAX(t) FROM timestamp;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2008-02-01 00:00:01"))}));

	// can't sum/avg timestamps
	REQUIRE_FAIL(con.Query("SELECT SUM(t) FROM timestamp"));
	REQUIRE_FAIL(con.Query("SELECT AVG(t) FROM timestamp"));
	// can't add/multiply/divide timestamps
	REQUIRE_FAIL(con.Query("SELECT t+t FROM timestamp"));
	REQUIRE_FAIL(con.Query("SELECT t*t FROM timestamp"));
	REQUIRE_FAIL(con.Query("SELECT t/t FROM timestamp"));
	REQUIRE_FAIL(con.Query("SELECT t%t FROM timestamp"));
	// FIXME: we can subtract timestamps!
	// REQUIRE_NO_FAIL(con.Query("SELECT t-t FROM timestamp"));

	// test YEAR function
	result = con.Query("SELECT YEAR(TIMESTAMP '1992-01-01 01:01:01');");
	REQUIRE(CHECK_COLUMN(result, 0, {1992}));
	result = con.Query("SELECT YEAR(TIMESTAMP '1992-01-01 01:01:01'::DATE);");
	REQUIRE(CHECK_COLUMN(result, 0, {1992}));
	// test casting timestamp
	result = con.Query("SELECT (TIMESTAMP '1992-01-01 01:01:01')::DATE;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1992, 1, 1)}));
	result = con.Query("SELECT (TIMESTAMP '1992-01-01 01:01:01')::TIME;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(1, 1, 1, 0)}));
	// scalar timestamp
	result = con.Query("SELECT t::DATE FROM timestamp WHERE EXTRACT(YEAR from t)=2007 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(2007, 1, 1)}));
	result = con.Query("SELECT t::TIME FROM timestamp WHERE EXTRACT(YEAR from t)=2007 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(0, 0, 1, 0)}));
	// date -> timestamp
	result = con.Query("SELECT (DATE '1992-01-01')::TIMESTAMP;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIMESTAMP(1992, 1, 1, 0, 0, 0, 0)}));

	// test timestamp with ms
	result = con.Query("SELECT TIMESTAMP '2008-01-01 00:00:01.5'::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {"2008-01-01 00:00:01.500"}));
	// test timestamp with BC date
	result = con.Query("SELECT TIMESTAMP '-8-01-01 00:00:01.5'::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {"0008-01-01 (BC) 00:00:01.500"}));
	// test timestamp with large date
	// FIXME:
	// result = con.Query("SELECT TIMESTAMP '100000-01-01 00:00:01.5'::VARCHAR");
	// REQUIRE(CHECK_COLUMN(result, 0, {"100000-01-01 (BC) 00:00:01.500"}));
}

TEST_CASE("Test out of range/incorrect timestamp formats", "[timestamp]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE timestamp(t TIMESTAMP)"));
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('blabla')"));
	// month out of range
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('1993-20-14 00:00:00')"));
	// day out of range
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('1993-08-99 00:00:00')"));
	// day out of range because not a leapyear
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('1993-02-29 00:00:00')"));
	// day out of range because not a leapyear
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('1900-02-29 00:00:00')"));
	// day in range because of leapyear
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamp VALUES ('1992-02-29 00:00:00')"));
	// day in range because of leapyear
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamp VALUES ('2000-02-29 00:00:00')"));

	// test incorrect timestamp formats
	// dd-mm-YYYY
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('02-02-1992 00:00:00')"));
	// ss-mm-hh
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('1900-1-1 59:59:23')"));
	// different separators are not supported
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('1900a01a01 00:00:00')"));
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('1900-1-1 00;00;00')"));
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('1900-1-1 00a00a00')"));
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('1900-1-1 00/00/00')"));
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('1900-1-1 00-00-00')"));
}

TEST_CASE("Test storage for timestamp type", "[timestamp]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_timestamp_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE timestamp (t TIMESTAMP);"));
		REQUIRE_NO_FAIL(con.Query(
		    "INSERT INTO timestamp VALUES ('2008-01-01 00:00:01'), (NULL), ('2007-01-01 00:00:01'), ('2008-02-01 "
		    "00:00:01'), "
		    "('2008-01-02 00:00:01'), ('2008-01-01 10:00:00'), ('2008-01-01 00:10:00'), ('2008-01-01 00:00:10')"));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT t FROM timestamp ORDER BY t;");
		REQUIRE(CHECK_COLUMN(result, 0,
		                     {Value(), Value::BIGINT(Timestamp::FromString("2007-01-01 00:00:01")),
		                      Value::BIGINT(Timestamp::FromString("2008-01-01 00:00:01")),
		                      Value::BIGINT(Timestamp::FromString("2008-01-01 00:00:10")),
		                      Value::BIGINT(Timestamp::FromString("2008-01-01 00:10:00")),
		                      Value::BIGINT(Timestamp::FromString("2008-01-01 10:00:00")),
		                      Value::BIGINT(Timestamp::FromString("2008-01-02 00:00:01")),
		                      Value::BIGINT(Timestamp::FromString("2008-02-01 00:00:01"))}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test timestamp functions", "[timestamp]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	result = con.Query("SELECT AGE(TIMESTAMP '1957-06-13');");
	auto current_timestamp = Timestamp::GetCurrentTimestamp();
	auto interval = Timestamp::GetDifference(Timestamp::FromString("1957-06-13"), current_timestamp);
	auto timestamp = Timestamp::IntervalToTimestamp(interval);
	auto years = timestamp.year;
	auto months = timestamp.month;
	auto days = timestamp.day;

	std::string output{""};
	if (years == 0 && months == 0 && days == 0) {
		output += "00:00:00";
	} else {
		if (years != 0) {
			output = std::to_string(years);
			output += " years ";
		}
		if (months != 0) {
			output += std::to_string(months);
			output += " mons ";
		}
		if (days != 0) {
			output += std::to_string(days);
			output += " days";
		}
	}
	REQUIRE(CHECK_COLUMN(result, 0, {output.c_str()}));

	result = con.Query("SELECT AGE(TIMESTAMP '2001-04-10', TIMESTAMP '1957-06-13');");
	REQUIRE(CHECK_COLUMN(result, 0, {"43 years 9 mons 27 days"}));

	result = con.Query("SELECT age(TIMESTAMP '2014-04-25', TIMESTAMP '2014-04-17');");
	REQUIRE(CHECK_COLUMN(result, 0, {"8 days"}));

	result = con.Query("SELECT age(TIMESTAMP '2014-04-25', TIMESTAMP '2014-01-01');");
	REQUIRE(CHECK_COLUMN(result, 0, {"3 mons 24 days"}));

	result = con.Query("SELECT age(TIMESTAMP '2019-06-11', TIMESTAMP '2019-06-11');");
	REQUIRE(CHECK_COLUMN(result, 0, {"00:00:00"}));

	result = con.Query(" SELECT age(timestamp '2019-06-11 12:00:00', timestamp '2019-07-11 11:00:00');");
	REQUIRE(CHECK_COLUMN(result, 0, {"-29 days -23:00:00"}));

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE timestamp(t1 TIMESTAMP, t2 TIMESTAMP)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamp VALUES('2001-04-10', '1957-06-13')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamp VALUES('2014-04-25', '2014-04-17')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamp VALUES('2014-04-25','2014-01-01')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamp VALUES('2019-06-11', '2019-06-11')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamp VALUES(NULL, '2019-06-11')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamp VALUES('2019-06-11', NULL)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamp VALUES(NULL, NULL)"));

	result = con.Query("SELECT AGE(t1, TIMESTAMP '1957-06-13') FROM timestamp;");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {{"43 years 9 mons 27 days"},
	                      {"56 years 10 mons 12 days"},
	                      {"56 years 10 mons 12 days"},
	                      {"61 years 11 mons 28 days"},
	                      {Value()},
	                      {"61 years 11 mons 28 days"},
	                      {Value()}}));

	result = con.Query("SELECT AGE(TIMESTAMP '2001-04-10', t2) FROM timestamp;");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {{"43 years 9 mons 27 days"},
	                      {"-13 years -7 days"},
	                      {"-12 years -8 mons -21 days"},
	                      {"-18 years -2 mons -1 days"},
	                      {"-18 years -2 mons -1 days"},
	                      {Value()},
	                      {Value()}}));

	result = con.Query("SELECT AGE(t1, t2) FROM timestamp;");
	REQUIRE(CHECK_COLUMN(
	    result, 0,
	    {{"43 years 9 mons 27 days"}, {"8 days"}, {"3 mons 24 days"}, {"00:00:00"}, {Value()}, {Value()}, {Value()}}));

	result = con.Query("SELECT AGE(t1, t2) FROM timestamp WHERE t1 > '2001-12-12';");
	REQUIRE(CHECK_COLUMN(result, 0, {{"8 days"}, {"3 mons 24 days"}, {"00:00:00"}, {Value()}}));

	// Test NULLS
	result = con.Query("SELECT AGE(NULL, NULL);");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT AGE(TIMESTAMP '1957-06-13', NULL);");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT AGE(NULL, TIMESTAMP '1957-06-13');");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
}

TEST_CASE("Test milliseconds with timestamps", "[timestamp]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	result = con.Query(
	    "SELECT CAST('2001-04-20 14:42:11.123' AS TIMESTAMP) a, CAST('2001-04-20 14:42:11.0' AS TIMESTAMP) b;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2001-04-20 14:42:11.123"))}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(Timestamp::FromString("2001-04-20 14:42:11"))}));
}

TEST_CASE("Test more timestamp functions", "[timestamp]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	result = con.Query("SELECT CAST(CURRENT_TIME AS STRING), CAST(CURRENT_DATE AS STRING), CAST(CURRENT_TIMESTAMP AS "
	                   "STRING), CAST(NOW() AS STRING)");
	REQUIRE(result->success);

	auto ds = result->Fetch();
	REQUIRE(ds->size() == 1);
	REQUIRE(ds->column_count() == 4);

	auto time = Time::FromString(ds->GetValue(0, 0).str_value);
	REQUIRE(time > 0);

	auto date = Date::FromString(ds->GetValue(1, 0).str_value);
	REQUIRE(date > 0);

	auto ts = Timestamp::FromString(ds->GetValue(2, 0).str_value);
	REQUIRE(ts > 0);

	auto ts2 = Timestamp::FromString(ds->GetValue(3, 0).str_value);
	REQUIRE(ts2 > 0);
}
