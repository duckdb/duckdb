#include "catch.hpp"
#include "main/appender.hpp"
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
		Appender appender(db, DEFAULT_SCHEMA, "integers");
		for (size_t i = 0; i < 2000; i++) {
			appender.BeginRow();
			appender.AppendInteger(1);
			appender.EndRow();
		}
		appender.Commit();
	}

	// check that the values have been added to the database
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2000}));

	// test a rollback of the appender
	{
		Appender appender2(db, DEFAULT_SCHEMA, "integers");
		// now append a bunch of values
		for (size_t i = 0; i < 2000; i++) {
			appender2.BeginRow();
			appender2.AppendInteger(1);
			appender2.EndRow();
		}
		appender2.Rollback();
	}

	// the data in the database should not be changed
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2000}));

	// test different types
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vals(i TINYINT, j SMALLINT, k BIGINT, l VARCHAR, m DECIMAL)"));

	// now append a bunch of values
	{
		Appender appender(db, DEFAULT_SCHEMA, "vals");
		for (size_t i = 0; i < 2000; i++) {
			appender.BeginRow();
			appender.AppendTinyInt(1);
			appender.AppendSmallInt(1);
			appender.AppendBigInt(1);
			appender.AppendString("hello");
			appender.AppendDouble(3.33);
			appender.EndRow();
		}
		appender.Commit();
	}

	// check that the values have been added to the database
	result = con.Query("SELECT l, SUM(k) FROM vals GROUP BY l");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {2000}));

	// now test various error conditions
	// too few values per row
	{
		Appender appender(db, DEFAULT_SCHEMA, "integers");
		appender.BeginRow();
		REQUIRE_THROWS(appender.EndRow());
		appender.Rollback();
	}
	// too many values per row
	{
		Appender appender(db, DEFAULT_SCHEMA, "integers");
		appender.BeginRow();
		appender.AppendValue(Value::INTEGER(2000));
		REQUIRE_THROWS(appender.AppendValue(Value::INTEGER(2000)));
		appender.Rollback();
	}
}
