#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test prepared statements in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	duckdb_result res;
	duckdb_prepared_statement stmt = nullptr;
	duckdb_state status;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	status = duckdb_prepare(tester.connection, "SELECT CAST($1 AS BIGINT)", &stmt);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(stmt != nullptr);

	status = duckdb_bind_boolean(stmt, 1, true);
	REQUIRE(status == DuckDBSuccess);

	// Parameter index 2 is out of bounds
	status = duckdb_bind_boolean(stmt, 2, true);
	REQUIRE(status == DuckDBError);

	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 1);
	duckdb_destroy_result(&res);

	duckdb_bind_int8(stmt, 1, 8);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 8);
	duckdb_destroy_result(&res);

	duckdb_bind_int16(stmt, 1, 16);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 16);
	duckdb_destroy_result(&res);

	duckdb_bind_int32(stmt, 1, 32);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 32);
	duckdb_destroy_result(&res);

	duckdb_bind_int64(stmt, 1, 64);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 64);
	duckdb_destroy_result(&res);

	duckdb_bind_hugeint(stmt, 1, duckdb_double_to_hugeint(64));
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_hugeint_to_double(duckdb_value_hugeint(&res, 0, 0)) == 64.0);
	duckdb_destroy_result(&res);

	// Fetching a DECIMAL from a non-DECIMAL result returns 0
	duckdb_decimal decimal = duckdb_double_to_decimal(634.3453, 7, 4);
	duckdb_bind_decimal(stmt, 1, decimal);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	duckdb_decimal result_decimal = duckdb_value_decimal(&res, 0, 0);
	REQUIRE(result_decimal.scale == 0);
	REQUIRE(result_decimal.width == 0);
	REQUIRE(result_decimal.value.upper == 0);
	REQUIRE(result_decimal.value.lower == 0);
	duckdb_destroy_result(&res);

	duckdb_bind_uint8(stmt, 1, 8);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_uint8(&res, 0, 0) == 8);
	duckdb_destroy_result(&res);

	duckdb_bind_uint16(stmt, 1, 8);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_uint16(&res, 0, 0) == 8);
	duckdb_destroy_result(&res);

	duckdb_bind_uint32(stmt, 1, 8);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_uint32(&res, 0, 0) == 8);
	duckdb_destroy_result(&res);

	duckdb_bind_uint64(stmt, 1, 8);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_uint64(&res, 0, 0) == 8);
	duckdb_destroy_result(&res);

	duckdb_bind_float(stmt, 1, 42.0);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 42);
	duckdb_destroy_result(&res);

	duckdb_bind_double(stmt, 1, 43.0);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 43);
	duckdb_destroy_result(&res);

	REQUIRE(duckdb_bind_float(stmt, 1, NAN) == DuckDBSuccess);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBError);
	duckdb_destroy_result(&res);

	REQUIRE(duckdb_bind_double(stmt, 1, NAN) == DuckDBSuccess);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBError);
	duckdb_destroy_result(&res);

	REQUIRE(duckdb_bind_varchar(stmt, 1, "\x80\x40\x41") == DuckDBError);
	duckdb_bind_varchar(stmt, 1, "44");
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 44);
	duckdb_destroy_result(&res);

	duckdb_bind_null(stmt, 1);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_nullmask_data(&res, 0)[0] == true);
	duckdb_destroy_result(&res);

	duckdb_destroy_prepare(&stmt);
	// again to make sure it does not crash
	duckdb_destroy_result(&res);
	duckdb_destroy_prepare(&stmt);

	status = duckdb_prepare(tester.connection, "SELECT CAST($1 AS VARCHAR)", &stmt);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(stmt != nullptr);

	// invalid unicode
	REQUIRE(duckdb_bind_varchar_length(stmt, 1, "\x80", 1) == DuckDBError);
	// we can bind null values, though!
	REQUIRE(duckdb_bind_varchar_length(stmt, 1, "\x00\x40\x41", 3) == DuckDBSuccess);
	duckdb_bind_varchar_length(stmt, 1, "hello world", 5);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	auto value = duckdb_value_varchar(&res, 0, 0);
	REQUIRE(string(value) == "hello");
	REQUIRE(duckdb_value_int8(&res, 0, 0) == 0);
	duckdb_free(value);
	duckdb_destroy_result(&res);

	duckdb_bind_blob(stmt, 1, "hello\0world", 11);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	value = duckdb_value_varchar(&res, 0, 0);
	REQUIRE(string(value) == "hello\\x00world");
	REQUIRE(duckdb_value_int8(&res, 0, 0) == 0);
	duckdb_free(value);
	duckdb_destroy_result(&res);

	duckdb_date_struct date_struct;
	date_struct.year = 1992;
	date_struct.month = 9;
	date_struct.day = 3;

	duckdb_bind_date(stmt, 1, duckdb_to_date(date_struct));
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	value = duckdb_value_varchar(&res, 0, 0);
	REQUIRE(string(value) == "1992-09-03");
	duckdb_free(value);
	duckdb_destroy_result(&res);

	duckdb_time_struct time_struct;
	time_struct.hour = 12;
	time_struct.min = 22;
	time_struct.sec = 33;
	time_struct.micros = 123400;

	duckdb_bind_time(stmt, 1, duckdb_to_time(time_struct));
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	value = duckdb_value_varchar(&res, 0, 0);
	REQUIRE(string(value) == "12:22:33.1234");
	duckdb_free(value);
	duckdb_destroy_result(&res);

	duckdb_timestamp_struct ts;
	ts.date = date_struct;
	ts.time = time_struct;

	duckdb_bind_timestamp(stmt, 1, duckdb_to_timestamp(ts));
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	value = duckdb_value_varchar(&res, 0, 0);
	REQUIRE(string(value) == "1992-09-03 12:22:33.1234");
	duckdb_free(value);
	duckdb_destroy_result(&res);

	duckdb_interval interval;
	interval.months = 3;
	interval.days = 0;
	interval.micros = 0;

	duckdb_bind_interval(stmt, 1, interval);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	value = duckdb_value_varchar(&res, 0, 0);
	REQUIRE(string(value) == "3 months");
	duckdb_free(value);
	duckdb_destroy_result(&res);

	duckdb_destroy_prepare(&stmt);

	status = duckdb_query(tester.connection, "CREATE TABLE a (i INTEGER)", NULL);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_prepare(tester.connection, "INSERT INTO a VALUES (?)", &stmt);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(stmt != nullptr);
	REQUIRE(duckdb_nparams(nullptr) == 0);
	REQUIRE(duckdb_nparams(stmt) == 1);
	REQUIRE(duckdb_param_type(nullptr, 0) == DUCKDB_TYPE_INVALID);
	REQUIRE(duckdb_param_type(stmt, 0) == DUCKDB_TYPE_INVALID);
	REQUIRE(duckdb_param_type(stmt, 1) == DUCKDB_TYPE_INTEGER);
	REQUIRE(duckdb_param_type(stmt, 2) == DUCKDB_TYPE_INVALID);

	for (int32_t i = 1; i <= 1000; i++) {
		duckdb_bind_int32(stmt, 1, i);
		status = duckdb_execute_prepared(stmt, nullptr);
		REQUIRE(status == DuckDBSuccess);
	}
	duckdb_destroy_prepare(&stmt);

	status = duckdb_prepare(tester.connection, "SELECT SUM(i)*$1-$2 FROM a", &stmt);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(stmt != nullptr);
	// clear bindings
	duckdb_bind_int32(stmt, 1, 2);
	REQUIRE(duckdb_clear_bindings(stmt) == DuckDBSuccess);

	// bind again will succeed
	duckdb_bind_int32(stmt, 1, 2);
	duckdb_bind_int32(stmt, 2, 1000);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int32(&res, 0, 0) == 1000000);
	duckdb_destroy_result(&res);
	duckdb_destroy_prepare(&stmt);

	// not-so-happy path
	status = duckdb_prepare(tester.connection, "SELECT XXXXX", &stmt);
	REQUIRE(status == DuckDBError);
	duckdb_destroy_prepare(&stmt);

	status = duckdb_prepare(tester.connection, "SELECT CAST($1 AS INTEGER)", &stmt);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(stmt != nullptr);

	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBError);
	duckdb_destroy_result(&res);
	duckdb_destroy_prepare(&stmt);

	// test duckdb_malloc explicitly
	auto malloced_data = duckdb_malloc(100);
	memcpy(malloced_data, "hello\0", 6);
	REQUIRE(string((char *)malloced_data) == "hello");
	duckdb_free(malloced_data);

	status = duckdb_prepare(tester.connection, "SELECT sum(i) FROM a WHERE i > ?", &stmt);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(stmt != nullptr);
	REQUIRE(duckdb_nparams(stmt) == 1);
	REQUIRE(duckdb_param_type(nullptr, 0) == DUCKDB_TYPE_INVALID);
	REQUIRE(duckdb_param_type(stmt, 1) == DUCKDB_TYPE_INTEGER);

	duckdb_destroy_prepare(&stmt);
}

TEST_CASE("Test prepared statements with named parameters in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	duckdb_result res;
	duckdb_prepared_statement stmt = nullptr;
	duckdb_state status;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	status = duckdb_prepare(tester.connection, "SELECT CAST($my_val AS BIGINT)", &stmt);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(stmt != nullptr);

	idx_t parameter_index;
	// test invalid name
	status = duckdb_bind_parameter_index(stmt, &parameter_index, "invalid");
	REQUIRE(status == DuckDBError);

	status = duckdb_bind_parameter_index(stmt, &parameter_index, "my_val");
	REQUIRE(status == DuckDBSuccess);

	idx_t param_count = duckdb_nparams(stmt);
	duckdb::vector<string> names;
	for (idx_t i = 0; i < param_count; i++) {
		auto name = duckdb_parameter_name(stmt, i + 1);
		names.push_back(std::string(name));
		duckdb_free((void *)name);
	}

	REQUIRE(duckdb_parameter_name(stmt, 0) == (const char *)NULL);
	REQUIRE(duckdb_parameter_name(stmt, 2) == (const char *)NULL);

	duckdb::vector<string> expected_names = {"my_val"};
	REQUIRE(names.size() == expected_names.size());
	for (idx_t i = 0; i < expected_names.size(); i++) {
		auto &name = names[i];
		auto &expected_name = expected_names[i];
		REQUIRE(name == expected_name);
	}

	status = duckdb_bind_boolean(stmt, parameter_index, 1);
	REQUIRE(status == DuckDBSuccess);
	status = duckdb_bind_boolean(stmt, parameter_index + 1, 1);
	REQUIRE(status == DuckDBError);

	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 1);
	duckdb_destroy_result(&res);

	// Clear the bindings, don't rebind the parameter index
	status = duckdb_clear_bindings(stmt);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_bind_boolean(stmt, parameter_index, 1);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 1);
	duckdb_destroy_result(&res);

	duckdb_destroy_prepare(&stmt);
}
