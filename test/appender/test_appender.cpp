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
		for (idx_t i = 0; i < 2000; i++) {
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
		for (idx_t i = 0; i < 2000; i++) {
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

		for (idx_t i = 0; i < 2000; i++) {
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
		for (idx_t i = 0; i < 2000; i++) {
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
		for (idx_t i = 0; i < 2000; i++) {
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

TEST_CASE("Test appending to a different database file", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto test_dir = TestDirectoryPath();
	auto attach_query = "ATTACH '" + test_dir + "/append_to_other.db'";
	REQUIRE_NO_FAIL(con.Query(attach_query));
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE append_to_other.tbl(i INTEGER)"));

	Appender appender(con, "append_to_other", "main", "tbl");
	for (idx_t i = 0; i < 200; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(2);
		appender.EndRow();
	}
	appender.Close();

	result = con.Query("SELECT SUM(i) FROM append_to_other.tbl");
	REQUIRE(CHECK_COLUMN(result, 0, {400}));
	bool failed;

	try {
		failed = false;
		Appender appender_invalid(con, "invalid_database", "main", "tbl");
	} catch (std::exception &ex) {
		ErrorData error(ex);
		REQUIRE(error.Message().find("Catalog Error") != std::string::npos);
		failed = true;
	}
	REQUIRE(failed);

	try {
		failed = false;
		Appender appender_invalid(con, "append_to_other", "invalid_schema", "tbl");
	} catch (std::exception &ex) {
		ErrorData error(ex);
		REQUIRE(error.Message().find("Catalog Error") != std::string::npos);
		failed = true;
	}
	REQUIRE(failed);

	// Attach as readonly.
	REQUIRE_NO_FAIL(con.Query("DETACH append_to_other"));
	REQUIRE_NO_FAIL(con.Query(attach_query + " (readonly)"));

	try {
		failed = false;
		Appender appender_readonly(con, "append_to_other", "main", "tbl");
	} catch (std::exception &ex) {
		ErrorData error(ex);
		REQUIRE(error.Message().find("Cannot append to a readonly database") != std::string::npos);
		failed = true;
	}
	REQUIRE(failed);
}

TEST_CASE("Test appending to different database files", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto test_dir = TestDirectoryPath();
	auto attach_db1 = "ATTACH '" + test_dir + "/db1.db'";
	auto attach_db2 = "ATTACH '" + test_dir + "/db2.db'";
	REQUIRE_NO_FAIL(con.Query(attach_db1));
	REQUIRE_NO_FAIL(con.Query(attach_db2));
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE db1.tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE db2.tbl(i INTEGER)"));

	REQUIRE_NO_FAIL(con.Query("START TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO db1.tbl VALUES (1)"));

	Appender appender(con, "db2", "main", "tbl");
	appender.BeginRow();
	appender.Append<int32_t>(2);
	appender.EndRow();

	bool failed;
	try {
		failed = false;
		appender.Close();
	} catch (std::exception &ex) {
		ErrorData error(ex);
		REQUIRE(error.Message().find("a single transaction can only write to a single attached database") !=
		        std::string::npos);
		failed = true;
	}
	REQUIRE(failed);
	REQUIRE_NO_FAIL(con.Query("COMMIT TRANSACTION"));
}

void setDataChunkInt32(DataChunk &chunk, idx_t col_idx, idx_t row_idx, int32_t value) {
	auto &col = chunk.data[col_idx];
	auto data = FlatVector::GetData<int32_t>(col);
	data[row_idx] = value;
}

TEST_CASE("Test appending with an active default column", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(
	    con.Query("CREATE OR REPLACE TABLE tbl (i INT DEFAULT 4, j INT, k INT DEFAULT 30, l AS (random()))"));
	Appender appender(con, "main", "tbl");
	appender.AddColumn("i");

	DataChunk chunk;
	const duckdb::vector<LogicalType> types = {LogicalType::INTEGER};
	chunk.Initialize(*con.context, types);

	setDataChunkInt32(chunk, 0, 0, 42);
	setDataChunkInt32(chunk, 0, 1, 43);

	chunk.SetCardinality(2);
	appender.AppendDataChunk(chunk);
	appender.Close();

	result = con.Query("SELECT i, j, k, l IS NOT NULL FROM tbl");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {30, 30}));
	REQUIRE(CHECK_COLUMN(result, 3, {true, true}));
}

TEST_CASE("Test appending with two active normal columns", "[appender]") {
#if STANDARD_VECTOR_SIZE != 2048
	return;
#endif
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(
	    con.Query("CREATE OR REPLACE TABLE tbl (i INT DEFAULT 4, j INT, k INT DEFAULT 30, l AS (2 * j), m INT)"));
	Appender appender(con, "main", "tbl");
	appender.AddColumn("j");
	appender.AddColumn("m");

	DataChunk chunk;
	const duckdb::vector<LogicalType> types = {LogicalType::INTEGER, LogicalType::INTEGER};
	chunk.Initialize(*con.context, types);

	for (idx_t i = 0; i < 4; i++) {
		for (idx_t j = 0; j < 2; j++) {
			auto &col = chunk.data[j];
			auto col_data = FlatVector::GetData<int32_t>(col);

			auto offset = i * STANDARD_VECTOR_SIZE;
			for (idx_t k = 0; k < STANDARD_VECTOR_SIZE; k++) {
				col_data[k] = int32_t(offset + k);
			}
		}
		chunk.SetCardinality(STANDARD_VECTOR_SIZE);
		appender.AppendDataChunk(chunk);
		chunk.Reset();
	}
	appender.Close();

	result = con.Query("SELECT SUM(i), SUM(j), SUM(k), SUM(l), SUM(m) FROM tbl");
	REQUIRE(CHECK_COLUMN(result, 0, {32768}));
	REQUIRE(CHECK_COLUMN(result, 1, {33550336}));
	REQUIRE(CHECK_COLUMN(result, 2, {245760}));
	REQUIRE(CHECK_COLUMN(result, 3, {67100672}));
	REQUIRE(CHECK_COLUMN(result, 4, {33550336}));
}

TEST_CASE("Test changing the active column configuration", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE tbl (i INT DEFAULT 4, j INT, k INT DEFAULT 30)"));
	Appender appender(con, "main", "tbl");

	// Create a data chunk with all three columns filled.

	DataChunk chunk_all_types;
	const duckdb::vector<LogicalType> all_types = {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER};
	chunk_all_types.Initialize(*con.context, all_types);

	setDataChunkInt32(chunk_all_types, 0, 0, 42);
	setDataChunkInt32(chunk_all_types, 1, 0, 111);
	setDataChunkInt32(chunk_all_types, 2, 0, 50);

	chunk_all_types.SetCardinality(1);
	appender.AppendDataChunk(chunk_all_types);

	appender.AddColumn("j");
	appender.AddColumn("i");

	DataChunk chunk_j_i;
	const duckdb::vector<LogicalType> types_j_i = {LogicalType::INTEGER, LogicalType::INTEGER};
	chunk_j_i.Initialize(*con.context, types_j_i);

	setDataChunkInt32(chunk_j_i, 0, 0, 111);
	setDataChunkInt32(chunk_j_i, 1, 0, 42);

	chunk_j_i.SetCardinality(1);
	appender.AppendDataChunk(chunk_j_i);

	appender.ClearColumns();
	appender.AppendDataChunk(chunk_all_types);

	appender.AddColumn("k");

	DataChunk chunk_k;
	const duckdb::vector<LogicalType> types_k = {LogicalType::INTEGER};
	chunk_k.Initialize(*con.context, types_k);

	setDataChunkInt32(chunk_k, 0, 0, 50);

	chunk_k.SetCardinality(1);
	appender.AppendDataChunk(chunk_k);
	appender.Close();

	result = con.Query("SELECT i, j, k FROM tbl");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 42, 42, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {111, 111, 111, Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {50, 30, 50, 50}));
}

TEST_CASE("Test edge cases for the active column configuration", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TABLE tbl (i INT DEFAULT 4, j INT, k INT DEFAULT 30, l AS (2 * j))"));
	Appender appender(con, "main", "tbl");

	appender.AddColumn("i");
	appender.AddColumn("j");

	bool failed;
	// Cannot add columns that do not exist.
	try {
		failed = false;
		appender.AddColumn("hello");
	} catch (std::exception &ex) {
		ErrorData error(ex);
		REQUIRE(error.Message().find("the column must exist in the table") != std::string::npos);
		failed = true;
	}
	REQUIRE(failed);

	// Cannot add generated columns.
	try {
		failed = false;
		appender.AddColumn("l");
	} catch (std::exception &ex) {
		ErrorData error(ex);
		REQUIRE(error.Message().find("cannot add a generated column to the appender") != std::string::npos);
		failed = true;
	}
	REQUIRE(failed);

	// Cannot add the same column twice.
	try {
		failed = false;
		appender.AddColumn("j");
	} catch (std::exception &ex) {
		ErrorData error(ex);
		REQUIRE(error.Message().find("cannot add the same column twice") != std::string::npos);
		failed = true;
	}
	REQUIRE(failed);
	appender.Close();
}

TEST_CASE("Test appending rows with an active column list", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(
	    con.Query("CREATE OR REPLACE TABLE tbl (i INT DEFAULT 4, j INT, k INT DEFAULT 30, l AS (2 * j), m INT)"));
	Appender appender(con, "main", "tbl");
	appender.AddColumn("j");
	appender.AddColumn("m");

	appender.Append(42);
	appender.Append(43);
	appender.EndRow();

	appender.Append(Value());
	appender.Append(44);
	appender.EndRow();
	appender.Close();

	result = con.Query("SELECT i, j, k, l, m FROM tbl");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {30, 30}));
	REQUIRE(CHECK_COLUMN(result, 3, {84, Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {43, 44}));
}
