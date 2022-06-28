#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;
using namespace std;

TEST_CASE("Test appender statements in C API", "[capi]") {
	CAPITester tester;
	unique_ptr<CAPIResult> result;
	duckdb_state status;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	tester.Query("CREATE TABLE test (i INTEGER, d double, s string)");
	duckdb_appender appender;

	status = duckdb_appender_create(tester.connection, nullptr, "nonexistant-table", &appender);
	REQUIRE(status == DuckDBError);
	REQUIRE(appender != nullptr);
	REQUIRE(duckdb_appender_error(appender) != nullptr);
	REQUIRE(duckdb_appender_destroy(&appender) == DuckDBSuccess);
	REQUIRE(duckdb_appender_destroy(nullptr) == DuckDBError);

	status = duckdb_appender_create(tester.connection, nullptr, "test", nullptr);
	REQUIRE(status == DuckDBError);

	status = duckdb_appender_create(tester.connection, nullptr, "test", &appender);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_appender_error(appender) == nullptr);

	status = duckdb_appender_begin_row(appender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_int32(appender, 42);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_double(appender, 4.2);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_varchar(appender, "Hello, World");
	REQUIRE(status == DuckDBSuccess);

	// out of cols here
	status = duckdb_append_int32(appender, 42);
	REQUIRE(status == DuckDBError);

	status = duckdb_appender_end_row(appender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_flush(appender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_begin_row(appender);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_int32(appender, 42);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_append_double(appender, 4.2);
	REQUIRE(status == DuckDBSuccess);

	// not enough cols here
	status = duckdb_appender_end_row(appender);
	REQUIRE(status == DuckDBError);
	REQUIRE(duckdb_appender_error(appender) != nullptr);

	status = duckdb_append_varchar(appender, "Hello, World");
	REQUIRE(status == DuckDBSuccess);

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
	                             "DATE, tm TIME, ts TIMESTAMP, ival INTERVAL, h HUGEINT)"));
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

	// double out of range for hugeint
	hugeint = duckdb_double_to_hugeint(1e300);
	REQUIRE(hugeint.lower == 0);
	REQUIRE(hugeint.upper == 0);

	hugeint = duckdb_double_to_hugeint(NAN);
	REQUIRE(hugeint.lower == 0);
	REQUIRE(hugeint.upper == 0);
}

TEST_CASE("Test append timestamp in C API", "[capi]") {
	CAPITester tester;
	unique_ptr<CAPIResult> result;
	duckdb_state status;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	tester.Query("CREATE TABLE test (t timestamp)");
	duckdb_appender appender;

	status = duckdb_appender_create(tester.connection, nullptr, "test", &appender);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_appender_error(appender) == nullptr);

	status = duckdb_appender_begin_row(appender);
	REQUIRE(status == DuckDBSuccess);

	// status = duckdb_append_timestamp(appender, duckdb_timestamp{1649519797544000});
	status = duckdb_append_varchar(appender, "2022-04-09 15:56:37.544");
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_appender_end_row(appender);
	REQUIRE(status == DuckDBSuccess);

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
