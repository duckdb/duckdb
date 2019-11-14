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
		auto appender = con.OpenAppender(DEFAULT_SCHEMA, "integers");
		for (size_t i = 0; i < 2000; i++) {
			appender->BeginRow();
			appender->AppendInteger(1);
			appender->EndRow();
		}
		con.CloseAppender();
	}

	con.Query("BEGIN TRANSACTION");

	// check that the values have been added to the database
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2000}));

	// test a rollback of the appender
	{
		auto appender2 = con.OpenAppender(DEFAULT_SCHEMA, "integers");
		// now append a bunch of values
		for (size_t i = 0; i < 2000; i++) {
			appender2->BeginRow();
			appender2->AppendInteger(1);
			appender2->EndRow();
		}
		con.CloseAppender();
	}
	con.Query("ROLLBACK");

	// the data in the database should not be changed
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2000}));

	// test different types
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vals(i TINYINT, j SMALLINT, k BIGINT, l VARCHAR, m DECIMAL)"));

	// now append a bunch of values
	{
		auto appender = con.OpenAppender(DEFAULT_SCHEMA, "vals");

		for (size_t i = 0; i < 2000; i++) {
			appender->BeginRow();
			appender->AppendTinyInt(1);
			appender->AppendSmallInt(1);
			appender->AppendBigInt(1);
			appender->AppendString("hello");
			appender->AppendDouble(3.33);
			appender->EndRow();
		}
		con.CloseAppender();
	}

	// check that the values have been added to the database
	result = con.Query("SELECT l, SUM(k) FROM vals GROUP BY l");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {2000}));

	// now test various error conditions
	// too few values per row
	{
		auto appender = con.OpenAppender(DEFAULT_SCHEMA, "integers");
		appender->BeginRow();
		REQUIRE_THROWS(appender->EndRow());
		con.CloseAppender();
	}
	// too many values per row
	{
		auto appender = con.OpenAppender(DEFAULT_SCHEMA, "integers");
		appender->BeginRow();
		appender->AppendValue(Value::INTEGER(2000));
		REQUIRE_THROWS(appender->AppendValue(Value::INTEGER(2000)));
		con.CloseAppender();
	}
}
