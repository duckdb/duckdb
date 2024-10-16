#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Basic appender tests", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
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
			appender.Append<const char *>("hello");
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

TEST_CASE("Test AppendRow", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));

	// append a bunch of values
	{
		Appender appender(con, "integers");
		for (size_t i = 0; i < 2000; i++) {
			appender.AppendRow(1);
		}
		appender.Close();
	}

	// check that the values have been added to the database
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2000}));

	{
		Appender appender(con, "integers");
		// test wrong types in append row
		REQUIRE_THROWS(appender.AppendRow("hello"));
	}

	// test different types
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vals(i TINYINT, j SMALLINT, k BIGINT, l VARCHAR, m DECIMAL)"));
	// now append a bunch of values
	{
		Appender appender(con, "vals");
		for (size_t i = 0; i < 2000; i++) {
			appender.AppendRow(1, 1, 1, "hello", 3.33);
			// append null values
			appender.AppendRow(nullptr, nullptr, nullptr, nullptr, nullptr);
		}
	}

	result = con.Query("SELECT COUNT(*), COUNT(i), COUNT(j), COUNT(k), COUNT(l), COUNT(m) FROM vals");
	REQUIRE(CHECK_COLUMN(result, 0, {4000}));
	REQUIRE(CHECK_COLUMN(result, 1, {2000}));
	REQUIRE(CHECK_COLUMN(result, 2, {2000}));
	REQUIRE(CHECK_COLUMN(result, 3, {2000}));
	REQUIRE(CHECK_COLUMN(result, 4, {2000}));
	REQUIRE(CHECK_COLUMN(result, 5, {2000}));

	// check that the values have been added to the database
	result = con.Query("SELECT l, SUM(k) FROM vals WHERE i IS NOT NULL GROUP BY l");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {2000}));

	// test dates and times
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dates(d DATE, t TIME, ts TIMESTAMP)"));
	// now append a bunch of values
	{
		Appender appender(con, "dates");
		appender.AppendRow(Value::DATE(1992, 1, 1), Value::TIME(1, 1, 1, 0), Value::TIMESTAMP(1992, 1, 1, 1, 1, 1, 0));
	}
	result = con.Query("SELECT * FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1992, 1, 1)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::TIME(1, 1, 1, 0)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::TIMESTAMP(1992, 1, 1, 1, 1, 1, 0)}));

	// test dates and times without value append
	REQUIRE_NO_FAIL(con.Query("DELETE FROM dates"));
	// now append a bunch of values
	{
		Appender appender(con, "dates");
		appender.AppendRow(Date::FromDate(1992, 1, 1), Time::FromTime(1, 1, 1, 0),
		                   Timestamp::FromDatetime(Date::FromDate(1992, 1, 1), Time::FromTime(1, 1, 1, 0)));
	}
	result = con.Query("SELECT * FROM dates");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DATE(1992, 1, 1)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::TIME(1, 1, 1, 0)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::TIMESTAMP(1992, 1, 1, 1, 1, 1, 0)}));
}

TEST_CASE("Test appender with generated column", "[appender]") {
	DuckDB db(nullptr); // Create an in-memory DuckDB database
	Connection con(db); // Create a connection to the database

	SECTION("Insert into table with generated column first") {
		// Try to create a table with a generated column
		REQUIRE_NOTHROW(con.Query(R"(
			CREATE TABLE tbl (
				b VARCHAR GENERATED ALWAYS AS (a),
				a VARCHAR
			)
		)"));

		Appender appender(con, "tbl");
		REQUIRE_NOTHROW(appender.BeginRow());
		REQUIRE_NOTHROW(appender.Append("a"));

		// Column 'b' is generated from 'a', so it does not need to be explicitly appended
		// End the row
		REQUIRE_NOTHROW(appender.EndRow());

		// Close the appender
		REQUIRE_NOTHROW(appender.Close());

		// Query the table to verify that the row was inserted correctly
		auto result = con.Query("SELECT * FROM tbl");
		REQUIRE_NO_FAIL(*result);

		// Check that the column 'a' contains "a" and 'b' contains the generated value "a"
		REQUIRE(CHECK_COLUMN(result, 0, {Value("a")}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value("a")}));
	}

	SECTION("Insert into table with generated column second") {
		// Try to create a table with a generated column
		REQUIRE_NOTHROW(con.Query(R"(
			CREATE TABLE tbl (
				a VARCHAR,
				b VARCHAR GENERATED ALWAYS AS (a)
			)
		)"));

		Appender appender(con, "tbl");
		REQUIRE_NOTHROW(appender.BeginRow());
		REQUIRE_NOTHROW(appender.Append("a"));

		// Column 'b' is generated from 'a', so it does not need to be explicitly appended
		// End the row
		REQUIRE_NOTHROW(appender.EndRow());

		// Close the appender
		REQUIRE_NOTHROW(appender.Close());

		// Query the table to verify that the row was inserted correctly
		auto result = con.Query("SELECT * FROM tbl");
		REQUIRE_NO_FAIL(*result);

		// Check that the column 'a' contains "a" and 'b' contains the generated value "a"
		REQUIRE(CHECK_COLUMN(result, 0, {Value("a")}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value("a")}));
	}
}

TEST_CASE("Test default value appender", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("Insert DEFAULT into default column") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i iNTEGER, j INTEGER DEFAULT 5)"));
		{
			Appender appender(con, "integers");
			appender.BeginRow();
			appender.Append<int32_t>(2);
			appender.AppendDefault();
			REQUIRE_NOTHROW(appender.EndRow());
			REQUIRE_NOTHROW(appender.Close());
		}
		result = con.Query("SELECT * FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(2)}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::INTEGER(5)}));
	}

	SECTION("Insert DEFAULT into non-default column") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i iNTEGER, j INTEGER DEFAULT 5)"));
		{
			Appender appender(con, "integers");
			appender.BeginRow();
			// 'i' does not have a DEFAULT value, so it gets NULL
			REQUIRE_NOTHROW(appender.AppendDefault());
			REQUIRE_NOTHROW(appender.AppendDefault());
			REQUIRE_NOTHROW(appender.EndRow());
			REQUIRE_NOTHROW(appender.Close());
		}
		result = con.Query("SELECT * FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(LogicalTypeId::INTEGER)}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::INTEGER(5)}));
	}

	SECTION("Insert DEFAULT into column that can't be NULL") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer NOT NULL)"));
		{
			Appender appender(con, "integers");
			appender.BeginRow();
			REQUIRE_NOTHROW(appender.AppendDefault());
			REQUIRE_NOTHROW(appender.EndRow());
			// NOT NULL constraint failed
			REQUIRE_THROWS(appender.Close());
		}
		result = con.Query("SELECT * FROM integers");
		auto chunk = result->Fetch();
		REQUIRE(chunk == nullptr);
	}

	SECTION("DEFAULT nextval('seq')") {
		REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i iNTEGER, j INTEGER DEFAULT nextval('seq'))"));
		{
			Appender appender(con, "integers");
			appender.BeginRow();
			appender.Append<int32_t>(1);

			// NOT_IMPLEMENTED: Non-foldable default values are not supported currently
			REQUIRE_THROWS(appender.AppendDefault());
			REQUIRE_THROWS(appender.EndRow());
			REQUIRE_NOTHROW(appender.Close());
		}
		// result = con.Query("SELECT * FROM integers");
		// REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(1)}));
		// REQUIRE(CHECK_COLUMN(result, 1, {Value::INTEGER(1)}));
	}

	SECTION("DEFAULT random()") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i iNTEGER, j DOUBLE DEFAULT random())"));
		con.Query("select setseed(0.42)");
		{
			Appender appender(con, "integers");
			appender.BeginRow();
			appender.Append<int32_t>(1);
			// NOT_IMPLEMENTED: Non-foldable default values are not supported currently
			REQUIRE_THROWS(appender.AppendDefault());
			REQUIRE_THROWS(appender.EndRow());
			REQUIRE_NOTHROW(appender.Close());
		}
		// result = con.Query("SELECT * FROM integers");
		// REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(1)}));
		// REQUIRE(CHECK_COLUMN(result, 1, {Value::DOUBLE(0.4729174713138491)}));
	}

	SECTION("DEFAULT now()") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i iNTEGER, j TIMESTAMPTZ DEFAULT now())"));
		con.Query("BEGIN TRANSACTION");
		result = con.Query("select now()");
		auto &materialized_result = result->Cast<MaterializedQueryResult>();
		auto current_time = materialized_result.GetValue(0, 0);
		{
			Appender appender(con, "integers");
			appender.BeginRow();
			appender.Append<int32_t>(1);
			REQUIRE_NOTHROW(appender.AppendDefault());
			REQUIRE_NOTHROW(appender.EndRow());
			REQUIRE_NOTHROW(appender.Close());
		}
		result = con.Query("SELECT * FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(1)}));
		REQUIRE(CHECK_COLUMN(result, 1, {current_time}));
		con.Query("COMMIT");
	}
}

TEST_CASE("Test incorrect usage of appender", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));

	// append a bunch of values
	{
		Appender appender(con, "integers");
		appender.BeginRow();
		appender.Append<int32_t>(1);
		// call EndRow before all rows have been appended results in an exception
		REQUIRE_THROWS(appender.EndRow());
		// we can still close the appender
		REQUIRE_NOTHROW(appender.Close());
	}
	{
		Appender appender(con, "integers");
		// flushing results in the same error
		appender.BeginRow();
		appender.Append<int32_t>(1);
		REQUIRE_THROWS(appender.Flush());
		// we can still close the appender
		REQUIRE_NOTHROW(appender.Close());
	}
	{
		// we get the same exception when calling AppendRow with an incorrect number of arguments
		Appender appender(con, "integers");
		REQUIRE_THROWS(appender.AppendRow(1));
		// we can still close the appender
		REQUIRE_NOTHROW(appender.Close());
	}
	{
		// we can flush an empty appender
		Appender appender(con, "integers");
		REQUIRE_NOTHROW(appender.Flush());
		REQUIRE_NOTHROW(appender.Flush());
		REQUIRE_NOTHROW(appender.Flush());
	}
}

TEST_CASE("Test appending NaN and INF using appender", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE doubles(d DOUBLE, f REAL)"));

	// appending NAN or INF succeeds
	Appender appender(con, "doubles");
	appender.AppendRow(1e308 + 1e308, 1e38f * 1e38f);
	appender.AppendRow(NAN, NAN);
	appender.Close();

	result = con.Query("SELECT * FROM doubles");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::DOUBLE(1e308 + 1e308), Value::DOUBLE(NAN)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::FLOAT(1e38f * 1e38f), Value::FLOAT(NAN)}));
}

TEST_CASE("Test appender with quotes", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA \"my_schema\""));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE \"my_schema\".\"my_table\"(\"i\" INTEGER)"));

	// append a bunch of values
	{
		Appender appender(con, "my_schema", "my_table");
		appender.AppendRow(1);
		appender.Close();
	}
	result = con.Query("SELECT * FROM my_schema.my_table");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test appender with string lengths", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE my_table (s STRING)"));
	{
		Appender appender(con, "my_table");
		appender.BeginRow();
		appender.Append("asdf", 3);
		appender.EndRow();
		appender.Close();
	}
	result = con.Query("SELECT * FROM my_table");
	REQUIRE(CHECK_COLUMN(result, 0, {"asd"}));
}

TEST_CASE("Test various appender types", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE type_table(a BOOL, b UINT8, c UINT16, d UINT32, e UINT64, f FLOAT)"));
	{
		Appender appender(con, "type_table");
		appender.AppendRow(true, uint8_t(1), uint16_t(2), uint32_t(3), uint64_t(4), 5.0f);
	}
	result = con.Query("SELECT * FROM type_table");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {2}));
	REQUIRE(CHECK_COLUMN(result, 3, {3}));
	REQUIRE(CHECK_COLUMN(result, 4, {4}));
	REQUIRE(CHECK_COLUMN(result, 5, {5}));
	// too many rows
	{
		Appender appender(con, "type_table");
		REQUIRE_THROWS(appender.AppendRow(true, uint8_t(1), uint16_t(2), uint32_t(3), uint64_t(4), 5.0f, nullptr));
	}
	{
		Appender appender(con, "type_table");
		REQUIRE_THROWS(appender.AppendRow(true, 1, 2, 3, 4, 5, 1));
	}
}

TEST_CASE("Test alter table in the middle of append", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	{
		// create the appender
		Appender appender(con, "integers");
		appender.AppendRow(1, 2);

		REQUIRE_NO_FAIL(con.Query("ALTER TABLE integers DROP COLUMN i"));
		REQUIRE_THROWS(appender.Close());
	}
}
