#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;
using namespace std;

namespace {

struct CAPIAppender {
public:
	CAPIAppender(CAPITester &tester, const char *schema, const char *table) {
		auto status = duckdb_appender_create(tester.connection, schema, table, &appender);
		REQUIRE(status == DuckDBSuccess);
	}
	~CAPIAppender() {
		auto status = duckdb_appender_close(appender);
		REQUIRE(status == DuckDBSuccess);
		duckdb_appender_destroy(&appender);
	}
	operator duckdb_appender() const {
		return appender;
	}
	operator duckdb_appender *() {
		return &appender;
	}

public:
	duckdb_appender appender = nullptr;
};

} // namespace

void TestAppenderError(duckdb_appender &appender, const string &expected) {
	auto error = duckdb_appender_error(appender);
	REQUIRE(error != nullptr);
	REQUIRE(duckdb::StringUtil::Contains(error, expected));
}

void AssertDecimalValueMatches(duckdb::unique_ptr<CAPIResult> &result, duckdb_decimal expected) {
	duckdb_decimal actual;

	actual = result->Fetch<duckdb_decimal>(0, 0);
	REQUIRE(actual.scale == expected.scale);
	REQUIRE(actual.width == expected.width);
	REQUIRE(actual.value.lower == expected.value.lower);
	REQUIRE(actual.value.upper == expected.value.upper);
}

template <class TYPE, duckdb_state APPEND_FUNC(duckdb_appender, TYPE)>
void TestAppendingSingleDecimalValue(TYPE value, duckdb_decimal expected, uint8_t width, uint8_t scale) {
	// Set the width and scale of the expected value
	expected.width = width;
	expected.scale = scale;

	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	duckdb_state status;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	tester.Query(StringUtil::Format("CREATE TABLE decimals(i DECIMAL(%d,%d))", width, scale));

	duckdb_appender appender;
	status = duckdb_appender_create(tester.connection, nullptr, "decimals", &appender);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_begin_row(appender);
	REQUIRE(status == DuckDBSuccess);

	status = APPEND_FUNC(appender, value);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_end_row(appender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_flush(appender);
	REQUIRE(status == DuckDBSuccess);

	result = tester.Query("SELECT * FROM decimals");
	AssertDecimalValueMatches(result, expected);

	duckdb_appender_close(appender);
	duckdb_appender_destroy(&appender);
}

TEST_CASE("Test appending into DECIMAL in C API", "[capi]") {
	duckdb_decimal expected;

	expected.value.lower = 1000;
	expected.value.upper = 0;
	TestAppendingSingleDecimalValue<int32_t, &duckdb_append_int32>(1, expected, 4, 3);
	expected.value.lower = (uint64_t)18446744073709541617UL;
	expected.value.upper = -1;
	TestAppendingSingleDecimalValue<int16_t, &duckdb_append_int16>(-9999, expected, 4, 0);
	expected.value.lower = 9999;
	expected.value.upper = 0;
	TestAppendingSingleDecimalValue<int16_t, &duckdb_append_int16>(9999, expected, 4, 0);
	expected.value.lower = 99999999;
	expected.value.upper = 0;
	TestAppendingSingleDecimalValue<int32_t, &duckdb_append_int32>(99999999, expected, 8, 0);
	expected.value.lower = 1234;
	expected.value.upper = 0;
	TestAppendingSingleDecimalValue<const char *, &duckdb_append_varchar>("1.234", expected, 4, 3);
	TestAppendingSingleDecimalValue<const char *, &duckdb_append_varchar>("123.4", expected, 4, 1);

	expected.value.lower = 3245234123123;
	expected.value.upper = 0;
	TestAppendingSingleDecimalValue<const char *, &duckdb_append_varchar>("3245234.123123", expected, 19, 6);
	TestAppendingSingleDecimalValue<const char *, &duckdb_append_varchar>("3245234.123123", expected, 13, 6);
	// Precision loss
	expected.value.lower = 123124320;
	expected.value.upper = 0;
	TestAppendingSingleDecimalValue<float, &duckdb_append_float>(12.3124324f, expected, 9, 7);

	// Precision loss
	expected.value.lower = 123452342343;
	expected.value.upper = 0;
	TestAppendingSingleDecimalValue<double, &duckdb_append_double>(12345234234.31243244234324, expected, 26, 1);
}

TEST_CASE("Test NULL struct Appender", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	duckdb_state status;

	REQUIRE(tester.OpenDatabase(nullptr));
	tester.Query("CREATE TABLE test (simple_struct STRUCT(a INTEGER, b VARCHAR));");
	duckdb_appender appender;

	status = duckdb_appender_create(tester.connection, nullptr, "test", &appender);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_appender_error(appender) == nullptr);

	// create the column types of the data chunk
	duckdb::vector<duckdb_logical_type> child_types = {duckdb_create_logical_type(DUCKDB_TYPE_INTEGER),
	                                                   duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR)};
	duckdb::vector<const char *> names = {"a", "b"};
	auto struct_type = duckdb_create_struct_type(child_types.data(), names.data(), child_types.size());

	// create the data chunk
	duckdb_logical_type types[1] = {struct_type};
	auto data_chunk = duckdb_create_data_chunk(types, 1);
	REQUIRE(data_chunk);
	REQUIRE(duckdb_data_chunk_get_column_count(data_chunk) == 1);

	auto struct_vector = duckdb_data_chunk_get_vector(data_chunk, 0);
	auto first_child_vector = duckdb_struct_vector_get_child(struct_vector, 0);
	auto second_child_vector = duckdb_struct_vector_get_child(struct_vector, 1);

	// set two values
	auto first_child_ptr = (int64_t *)duckdb_vector_get_data(first_child_vector);
	*first_child_ptr = 42;
	duckdb_vector_assign_string_element(second_child_vector, 0, "hello");

	// set the parent/STRUCT vector to NULL
	duckdb_vector_ensure_validity_writable(struct_vector);
	auto validity = duckdb_vector_get_validity(struct_vector);
	duckdb_validity_set_row_validity(validity, 1, false);

	// set the first child vector to NULL
	duckdb_vector_ensure_validity_writable(first_child_vector);
	auto first_validity = duckdb_vector_get_validity(first_child_vector);
	duckdb_validity_set_row_validity(first_validity, 1, false);

	// set the second child vector to NULL
	duckdb_vector_ensure_validity_writable(second_child_vector);
	auto second_validity = duckdb_vector_get_validity(second_child_vector);
	duckdb_validity_set_row_validity(second_validity, 1, false);

	duckdb_data_chunk_set_size(data_chunk, 2);
	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);

	// close flushes all remaining data
	status = duckdb_appender_close(appender);
	REQUIRE(status == DuckDBSuccess);

	// verify the result
	result = tester.Query("SELECT simple_struct::VARCHAR FROM test;");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<string>(0, 0) == "{'a': 42, 'b': hello}");
	REQUIRE(result->Fetch<string>(0, 1) == "");

	// destroy the data chunk and the appender
	duckdb_destroy_data_chunk(&data_chunk);
	status = duckdb_appender_destroy(&appender);
	REQUIRE(status == DuckDBSuccess);

	// destroy the logical types
	for (auto child_type : child_types) {
		duckdb_destroy_logical_type(&child_type);
	}
	duckdb_destroy_logical_type(&struct_type);
}

TEST_CASE("Test appender statements in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	duckdb_state status;

	REQUIRE(tester.OpenDatabase(nullptr));
	tester.Query("CREATE TABLE test (i INTEGER, d double, s string)");
	duckdb_appender appender;

	// Creating the table with an unknown table fails, but creates an appender object.
	REQUIRE(duckdb_appender_create(tester.connection, nullptr, "unknown_table", &appender) == DuckDBError);
	REQUIRE(appender != nullptr);
	TestAppenderError(appender, "could not be found");

	// Flushing, closing, or destroying the appender also fails due to its invalid table.
	REQUIRE(duckdb_appender_close(appender) == DuckDBError);
	TestAppenderError(appender, "could not be found");

	// Any data is still destroyed, so there are no leaks, even if duckdb_appender_destroy returns DuckDBError.
	REQUIRE(duckdb_appender_destroy(&appender) == DuckDBError);
	REQUIRE(duckdb_appender_destroy(nullptr) == DuckDBError);

	// Appender creation also fails if not providing an appender object.
	REQUIRE(duckdb_appender_create(tester.connection, nullptr, "test", nullptr) == DuckDBError);

	// Now, create a valid appender.
	REQUIRE(duckdb_appender_create(tester.connection, nullptr, "test", &appender) == DuckDBSuccess);
	REQUIRE(duckdb_appender_error(appender) == nullptr);

	// Start appending rows.
	REQUIRE(duckdb_appender_begin_row(appender) == DuckDBSuccess);
	REQUIRE(duckdb_append_int32(appender, 42) == DuckDBSuccess);
	REQUIRE(duckdb_append_double(appender, 4.2) == DuckDBSuccess);
	REQUIRE(duckdb_append_varchar(appender, "Hello, World") == DuckDBSuccess);

	// Exceed the column count.
	REQUIRE(duckdb_append_int32(appender, 42) == DuckDBError);
	TestAppenderError(appender, "Too many appends for chunk");

	// Finish and flush the row.
	REQUIRE(duckdb_appender_end_row(appender) == DuckDBSuccess);
	REQUIRE(duckdb_appender_flush(appender) == DuckDBSuccess);

	// Next row.
	REQUIRE(duckdb_appender_begin_row(appender) == DuckDBSuccess);
	REQUIRE(duckdb_append_int32(appender, 42) == DuckDBSuccess);
	REQUIRE(duckdb_append_double(appender, 4.2) == DuckDBSuccess);

	// Missing column.
	REQUIRE(duckdb_appender_end_row(appender) == DuckDBError);
	REQUIRE(duckdb_appender_error(appender) != nullptr);
	TestAppenderError(appender, "Call to EndRow before all columns have been appended to");

	// Append the missing column.
	REQUIRE(duckdb_append_varchar(appender, "Hello, World") == DuckDBSuccess);

	// out of cols here
	status = duckdb_append_int32(appender, 42);
	REQUIRE(status == DuckDBError);

	REQUIRE(duckdb_appender_error(appender) != nullptr);

	status = duckdb_appender_end_row(appender);
	REQUIRE(status == DuckDBSuccess);

	// we can flush again why not
	status = duckdb_appender_flush(appender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_close(appender);
	REQUIRE(status == DuckDBSuccess);

	result = tester.Query("SELECT * FROM test");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int32_t>(0, 0) == 42);
	REQUIRE(result->Fetch<double>(1, 0) == 4.2);
	REQUIRE(result->Fetch<string>(2, 0) == "Hello, World");

	status = duckdb_appender_destroy(&appender);
	REQUIRE(status == DuckDBSuccess);

	// this has been destroyed

	status = duckdb_appender_close(appender);
	REQUIRE(status == DuckDBError);
	REQUIRE(duckdb_appender_error(appender) == nullptr);

	status = duckdb_appender_flush(appender);
	REQUIRE(status == DuckDBError);

	status = duckdb_appender_end_row(appender);
	REQUIRE(status == DuckDBError);

	status = duckdb_append_int32(appender, 42);
	REQUIRE(status == DuckDBError);

	status = duckdb_appender_destroy(&appender);
	REQUIRE(status == DuckDBError);

	status = duckdb_appender_close(nullptr);
	REQUIRE(status == DuckDBError);

	status = duckdb_appender_flush(nullptr);
	REQUIRE(status == DuckDBError);

	status = duckdb_appender_end_row(nullptr);
	REQUIRE(status == DuckDBError);

	status = duckdb_append_int32(nullptr, 42);
	REQUIRE(status == DuckDBError);

	status = duckdb_appender_destroy(nullptr);
	REQUIRE(status == DuckDBError);

	status = duckdb_appender_destroy(nullptr);
	REQUIRE(status == DuckDBError);

	// many types
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE many_types(bool boolean, t TINYINT, s SMALLINT, b BIGINT, ut UTINYINT, "
	                             "us USMALLINT, ui UINTEGER, ub UBIGINT, uf REAL, ud DOUBLE, txt VARCHAR, blb BLOB, dt "
	                             "DATE, tm TIME, ts TIMESTAMP, ival INTERVAL, h HUGEINT, uh UHUGEINT)"));
	duckdb_appender tappender;

	status = duckdb_appender_create(tester.connection, nullptr, "many_types", &tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_begin_row(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_bool(tappender, true);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_int8(tappender, 1);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_int16(tappender, 1);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_int64(tappender, 1);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_uint8(tappender, 1);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_uint16(tappender, 1);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_uint32(tappender, 1);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_uint64(tappender, 1);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_float(tappender, 0.5f);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_double(tappender, 0.5);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_varchar_length(tappender, "hello world", 5);
	REQUIRE(status == DuckDBSuccess);

	duckdb_date_struct date_struct;
	date_struct.year = 1992;
	date_struct.month = 9;
	date_struct.day = 3;

	char blob_data[] = "hello world this\0is my long string";
	idx_t blob_len = 34;
	status = duckdb_append_blob(tappender, blob_data, blob_len);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_date(tappender, duckdb_to_date(date_struct));
	REQUIRE(status == DuckDBSuccess);

	duckdb_time_struct time_struct;
	time_struct.hour = 12;
	time_struct.min = 22;
	time_struct.sec = 33;
	time_struct.micros = 1234;

	status = duckdb_append_time(tappender, duckdb_to_time(time_struct));
	REQUIRE(status == DuckDBSuccess);

	duckdb_timestamp_struct ts;
	ts.date = date_struct;
	ts.time = time_struct;

	status = duckdb_append_timestamp(tappender, duckdb_to_timestamp(ts));
	REQUIRE(status == DuckDBSuccess);

	duckdb_interval interval;
	interval.months = 3;
	interval.days = 0;
	interval.micros = 0;

	status = duckdb_append_interval(tappender, interval);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_hugeint(tappender, duckdb_double_to_hugeint(27));
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_uhugeint(tappender, duckdb_double_to_uhugeint(27));
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_end_row(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_begin_row(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_null(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_end_row(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_flush(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_close(tappender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_destroy(&tappender);
	REQUIRE(status == DuckDBSuccess);

	result = tester.Query("SELECT * FROM many_types");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<bool>(0, 0) == true);
	REQUIRE(result->Fetch<int8_t>(1, 0) == 1);
	REQUIRE(result->Fetch<int16_t>(2, 0) == 1);
	REQUIRE(result->Fetch<int64_t>(3, 0) == 1);
	REQUIRE(result->Fetch<uint8_t>(4, 0) == 1);
	REQUIRE(result->Fetch<uint16_t>(5, 0) == 1);
	REQUIRE(result->Fetch<uint32_t>(6, 0) == 1);
	REQUIRE(result->Fetch<uint64_t>(7, 0) == 1);
	REQUIRE(result->Fetch<float>(8, 0) == 0.5f);
	REQUIRE(result->Fetch<double>(9, 0) == 0.5);
	REQUIRE(result->Fetch<string>(10, 0) == "hello");

	auto blob = duckdb_value_blob(&result->InternalResult(), 11, 0);
	REQUIRE(blob.size == blob_len);
	REQUIRE(memcmp(blob.data, blob_data, blob_len) == 0);
	duckdb_free(blob.data);
	REQUIRE(duckdb_value_int32(&result->InternalResult(), 11, 0) == 0);

	auto date = result->Fetch<duckdb_date_struct>(12, 0);
	REQUIRE(date.year == 1992);
	REQUIRE(date.month == 9);
	REQUIRE(date.day == 3);

	auto time = result->Fetch<duckdb_time_struct>(13, 0);
	REQUIRE(time.hour == 12);
	REQUIRE(time.min == 22);
	REQUIRE(time.sec == 33);
	REQUIRE(time.micros == 1234);

	auto timestamp = result->Fetch<duckdb_timestamp_struct>(14, 0);
	REQUIRE(timestamp.date.year == 1992);
	REQUIRE(timestamp.date.month == 9);
	REQUIRE(timestamp.date.day == 3);
	REQUIRE(timestamp.time.hour == 12);
	REQUIRE(timestamp.time.min == 22);
	REQUIRE(timestamp.time.sec == 33);
	REQUIRE(timestamp.time.micros == 1234);

	interval = result->Fetch<duckdb_interval>(15, 0);
	REQUIRE(interval.months == 3);
	REQUIRE(interval.days == 0);
	REQUIRE(interval.micros == 0);

	auto hugeint = result->Fetch<duckdb_hugeint>(16, 0);
	REQUIRE(duckdb_hugeint_to_double(hugeint) == 27);

	auto uhugeint = result->Fetch<duckdb_uhugeint>(17, 0);
	REQUIRE(duckdb_uhugeint_to_double(uhugeint) == 27);

	REQUIRE(result->IsNull(0, 1));
	REQUIRE(result->IsNull(1, 1));
	REQUIRE(result->IsNull(2, 1));
	REQUIRE(result->IsNull(3, 1));
	REQUIRE(result->IsNull(4, 1));
	REQUIRE(result->IsNull(5, 1));
	REQUIRE(result->IsNull(6, 1));
	REQUIRE(result->IsNull(7, 1));
	REQUIRE(result->IsNull(8, 1));
	REQUIRE(result->IsNull(9, 1));
	REQUIRE(result->IsNull(10, 1));
	REQUIRE(result->IsNull(11, 1));
	REQUIRE(result->IsNull(12, 1));
	REQUIRE(result->IsNull(13, 1));
	REQUIRE(result->IsNull(14, 1));
	REQUIRE(result->IsNull(15, 1));
	REQUIRE(result->IsNull(16, 1));

	REQUIRE(result->Fetch<bool>(0, 1) == false);
	REQUIRE(result->Fetch<int8_t>(1, 1) == 0);
	REQUIRE(result->Fetch<int16_t>(2, 1) == 0);
	REQUIRE(result->Fetch<int64_t>(3, 1) == 0);
	REQUIRE(result->Fetch<uint8_t>(4, 1) == 0);
	REQUIRE(result->Fetch<uint16_t>(5, 1) == 0);
	REQUIRE(result->Fetch<uint32_t>(6, 1) == 0);
	REQUIRE(result->Fetch<uint64_t>(7, 1) == 0);
	REQUIRE(result->Fetch<float>(8, 1) == 0);
	REQUIRE(result->Fetch<double>(9, 1) == 0);
	REQUIRE(result->Fetch<string>(10, 1).empty());

	blob = duckdb_value_blob(&result->InternalResult(), 11, 1);
	REQUIRE(blob.size == 0);

	date = result->Fetch<duckdb_date_struct>(12, 1);
	REQUIRE(date.year == 1970);

	time = result->Fetch<duckdb_time_struct>(13, 1);
	REQUIRE(time.hour == 0);

	timestamp = result->Fetch<duckdb_timestamp_struct>(14, 1);
	REQUIRE(timestamp.date.year == 1970);
	REQUIRE(timestamp.time.hour == 0);

	interval = result->Fetch<duckdb_interval>(15, 1);
	REQUIRE(interval.months == 0);

	hugeint = result->Fetch<duckdb_hugeint>(16, 1);
	REQUIRE(duckdb_hugeint_to_double(hugeint) == 0);

	uhugeint = result->Fetch<duckdb_uhugeint>(17, 1);
	REQUIRE(duckdb_uhugeint_to_double(uhugeint) == 0);

	// double out of range for hugeint
	hugeint = duckdb_double_to_hugeint(1e300);
	REQUIRE(hugeint.lower == 0);
	REQUIRE(hugeint.upper == 0);

	hugeint = duckdb_double_to_hugeint(NAN);
	REQUIRE(hugeint.lower == 0);
	REQUIRE(hugeint.upper == 0);

	// double out of range for uhugeint
	uhugeint = duckdb_double_to_uhugeint(1e300);
	REQUIRE(uhugeint.lower == 0);
	REQUIRE(uhugeint.upper == 0);

	uhugeint = duckdb_double_to_uhugeint(NAN);
	REQUIRE(uhugeint.lower == 0);
	REQUIRE(uhugeint.upper == 0);
}

TEST_CASE("Test append DEFAULT in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));

	SECTION("BASIC DEFAULT VALUE") {
		tester.Query("CREATE OR REPLACE TABLE test (a INTEGER, b INTEGER DEFAULT 5)");
		{
			CAPIAppender appender(tester, nullptr, "test");
			auto status = duckdb_appender_begin_row(appender);
			REQUIRE(status == DuckDBSuccess);
			duckdb_append_int32(appender, 42);
			// Even though the column has a DEFAULT, we still require explicitly appending a value/default
			status = duckdb_appender_end_row(appender);
			REQUIRE(status == DuckDBError);
			status = duckdb_appender_flush(appender);
			REQUIRE(status == DuckDBError);

			status = duckdb_append_default(appender);
			REQUIRE(status == DuckDBSuccess);
			status = duckdb_appender_end_row(appender);
			REQUIRE(status == DuckDBSuccess);
		}
		result = tester.Query("SELECT * FROM test");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->Fetch<int32_t>(0, 0) == 42);
		REQUIRE(result->Fetch<int32_t>(1, 0) == 5);
	}

	SECTION("NON DEFAULT VALUE") {
		tester.Query("CREATE OR REPLACE TABLE test (a INTEGER, b INTEGER DEFAULT 5)");
		{
			CAPIAppender appender(tester, nullptr, "test");
			auto status = duckdb_appender_begin_row(appender);
			REQUIRE(status == DuckDBSuccess);

			// Append default to column without a default
			status = duckdb_append_default(appender);
			REQUIRE(status == DuckDBSuccess);

			status = duckdb_append_int32(appender, 42);
			REQUIRE(status == DuckDBSuccess);
			status = duckdb_appender_end_row(appender);
			REQUIRE(status == DuckDBSuccess);
		}
		result = tester.Query("SELECT * FROM test");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->IsNull(0, 0));
		REQUIRE(result->Fetch<int32_t>(1, 0) == 42);
	}
}

TEST_CASE("Test append timestamp in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	duckdb_state status;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	tester.Query("CREATE TABLE test (t timestamp)");
	duckdb_appender appender;

	status = duckdb_appender_create(tester.connection, nullptr, "test", &appender);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_appender_error(appender) == nullptr);

	// successful append
	status = duckdb_appender_begin_row(appender);
	REQUIRE(status == DuckDBSuccess);

	// status = duckdb_append_timestamp(appender, duckdb_timestamp{1649519797544000});
	status = duckdb_append_varchar(appender, "2022-04-09 15:56:37.544");
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_end_row(appender);
	REQUIRE(status == DuckDBSuccess);

	// append failure
	status = duckdb_appender_begin_row(appender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_varchar(appender, "XXXXX");
	REQUIRE(status == DuckDBError);
	REQUIRE(duckdb_appender_error(appender) != nullptr);

	status = duckdb_appender_end_row(appender);
	REQUIRE(status == DuckDBError);

	status = duckdb_appender_flush(appender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_close(appender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_destroy(&appender);
	REQUIRE(status == DuckDBSuccess);

	result = tester.Query("SELECT * FROM test");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<string>(0, 0) == "2022-04-09 15:56:37.544");
}
