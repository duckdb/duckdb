#include "catch.hpp"
#include "common/types/timestamp.hpp"
#include "test_helpers.hpp"

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
	REQUIRE(result->sql_types[0] == SQLType(SQLTypeId::TIMESTAMP));
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

	// sum can be done only with time zone
	// result = con.Query("SELECT SUM(t) FROM timestamp;");
	// REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2017-07-23 13:10:11"))}));

	result = con.Query("SELECT MIN(t) FROM timestamp;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2007-01-01 00:00:01"))}));

	result = con.Query("SELECT MAX(t) FROM timestamp;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2008-02-01 00:00:01"))}));
}

TEST_CASE("Test out of range/incorrect timestamp formats", "[timestamp]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

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
	REQUIRE_FAIL(con.Query("INSERT INTO timestamp VALUES ('1900/01/01 00:00:00')"));
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
	{
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
	// reload the database from disk, we do this again because checkpointing at startup causes this to follow a
	// different code path
	{
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
