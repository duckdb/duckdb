#include "catch.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"

using namespace duckdb;
using namespace std;


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

TEST_CASE("Test storage for interval type", "[interval]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_interval_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE interval (t INTERVAL);"));
		REQUIRE_NO_FAIL(con.Query(
		    "INSERT INTO interval VALUES (INTERVAL '1' DAY), (NULL), (INTERVAL '3 months 2 days 5 seconds')"));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT t FROM interval ORDER BY t;");
		REQUIRE(CHECK_COLUMN(result, 0,
		                     {Value(), Value::INTERVAL(0, 1, 0), Value::INTERVAL(3, 2, 5000)}));
		result = con.Query("SELECT t FROM interval WHERE t = INTERVAL '1' DAY;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 1, 0)}));
		result = con.Query("SELECT t FROM interval WHERE t >= INTERVAL '1' DAY ORDER BY 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::INTERVAL(0, 1, 0), Value::INTERVAL(3, 2, 5000)}));
	}
	DeleteDatabase(storage_database);
}
