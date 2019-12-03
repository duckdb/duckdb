#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Basic appender tests", "[appender]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));

	// append a bunch of values
	{
		Appender appender(con, "integers");
		for (size_t i = 0; i < 2000; i++) {
			appender.BeginRow();
			appender.Append<int32_t>(1);
			appender.EndRow();
		}
		appender.Close();
	}

	con.Query("BEGIN TRANSACTION");

	// check that the values have been added to the database
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2000}));

	// test a rollback of the appender
	{
		Appender appender2(con, "integers");
		// now append a bunch of values
		for (size_t i = 0; i < 2000; i++) {
			appender2.BeginRow();
			appender2.Append<int32_t>(1);
			appender2.EndRow();
		}
		appender2.Close();
	}
	con.Query("ROLLBACK");

	// the data in the database should not be changed
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2000}));

	// test different types
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vals(i TINYINT, j SMALLINT, k BIGINT, l VARCHAR, m DECIMAL)"));

	// now append a bunch of values
	{
		Appender appender(con, "vals");

		for (size_t i = 0; i < 2000; i++) {
			appender.BeginRow();
			appender.Append<int8_t>(1);
			appender.Append<int16_t>(1);
			appender.Append<int64_t>(1);
			appender.Append<const char*>("hello");
			appender.Append<double>(3.33);
			appender.EndRow();
		}
	}

	// check that the values have been added to the database
	result = con.Query("SELECT l, SUM(k) FROM vals GROUP BY l");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {2000}));

	// now test various error conditions
	// too few values per row
	{
		Appender appender(con, "integers");
		appender.BeginRow();
		REQUIRE_THROWS(appender.EndRow());
	}
	// too many values per row
	{
		Appender appender(con, "integers");
		appender.BeginRow();
		appender.Append<Value>(Value::INTEGER(2000));
		REQUIRE_THROWS(appender.Append<Value>(Value::INTEGER(2000)));
	}
}
