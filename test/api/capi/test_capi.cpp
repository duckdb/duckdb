#include "catch.hpp"
#include "duckdb.h"
#include "test_helpers.hpp"
#include "duckdb/common/arrow.hpp"
#include "duckdb/common/exception.hpp"

using namespace duckdb;
using namespace std;

class CAPIResult {
public:
	~CAPIResult() {
		duckdb_destroy_result(&result);
	}
	void Query(duckdb_connection connection, string query) {
		success = (duckdb_query(connection, query.c_str(), &result) == DuckDBSuccess);
	}

	duckdb_type ColumnType(idx_t col) {
		return duckdb_column_type(&result, col);
	}

	template <class T>
	T *ColumnData(idx_t col) {
		return (T *)duckdb_column_data(&result, col);
	}

	idx_t ColumnCount() {
		return duckdb_column_count(&result);
	}

	idx_t row_count() {
		return duckdb_row_count(&result);
	}

	idx_t rows_changed() {
		return duckdb_rows_changed(&result);
	}

	template <class T>
	T Fetch(idx_t col, idx_t row) {
		throw NotImplementedException("Unimplemented type for fetch");
	}

	bool IsNull(idx_t col, idx_t row) {
		auto nullmask_ptr = duckdb_nullmask_data(&result, col);
		REQUIRE(duckdb_value_is_null(&result, col, row) == nullmask_ptr[row]);
		return nullmask_ptr[row];
	}

	char *ErrorMessage() {
		return duckdb_result_error(&result);
	}

	string ColumnName(idx_t col) {
		auto colname = duckdb_column_name(&result, col);
		return colname ? string(colname) : string();
	}

	duckdb_result &InternalResult() {
		return result;
	}

public:
	bool success = false;

private:
	duckdb_result result;
};

static bool NO_FAIL(CAPIResult &result) {
	return result.success;
}

static bool NO_FAIL(unique_ptr<CAPIResult> result) {
	return NO_FAIL(*result);
}

template <>
bool CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_boolean(&result, col, row);
}

template <>
int8_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int8(&result, col, row);
}

template <>
int16_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int16(&result, col, row);
}

template <>
int32_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int32(&result, col, row);
}

template <>
int64_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int64(&result, col, row);
}

template <>
uint8_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_uint8(&result, col, row);
}

template <>
uint16_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_uint16(&result, col, row);
}

template <>
uint32_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_uint32(&result, col, row);
}

template <>
uint64_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_uint64(&result, col, row);
}

template <>
float CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_float(&result, col, row);
}

template <>
double CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_double(&result, col, row);
}

template <>
duckdb_date CAPIResult::Fetch(idx_t col, idx_t row) {
	auto data = (duckdb_date *)result.columns[col].data;
	return data[row];
}

template <>
duckdb_time CAPIResult::Fetch(idx_t col, idx_t row) {
	auto data = (duckdb_time *)result.columns[col].data;
	return data[row];
}

template <>
duckdb_timestamp CAPIResult::Fetch(idx_t col, idx_t row) {
	auto data = (duckdb_timestamp *)result.columns[col].data;
	return data[row];
}

template <>
duckdb_interval CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_interval(&result, col, row);
}

template <>
duckdb_blob CAPIResult::Fetch(idx_t col, idx_t row) {
	auto data = (duckdb_blob *)result.columns[col].data;
	return data[row];
}

template <>
string CAPIResult::Fetch(idx_t col, idx_t row) {
	auto value = duckdb_value_varchar(&result, col, row);
	string strval = value ? string(value) : string();
	free((void *)value);
	return strval;
}

template <>
duckdb_date_struct CAPIResult::Fetch(idx_t col, idx_t row) {
	auto value = duckdb_value_date(&result, col, row);
	return duckdb_from_date(value);
}

template <>
duckdb_time_struct CAPIResult::Fetch(idx_t col, idx_t row) {
	auto value = duckdb_value_time(&result, col, row);
	return duckdb_from_time(value);
}

template <>
duckdb_timestamp_struct CAPIResult::Fetch(idx_t col, idx_t row) {
	auto value = duckdb_value_timestamp(&result, col, row);
	return duckdb_from_timestamp(value);
}

template <>
duckdb_hugeint CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_hugeint(&result, col, row);
}

class CAPITester {
public:
	CAPITester() : database(nullptr), connection(nullptr) {
	}
	~CAPITester() {
		Cleanup();
	}

	void Cleanup() {
		if (connection) {
			duckdb_disconnect(&connection);
			connection = nullptr;
		}
		if (database) {
			duckdb_close(&database);
			database = nullptr;
		}
	}

	bool OpenDatabase(const char *path) {
		Cleanup();
		if (duckdb_open(path, &database) != DuckDBSuccess) {
			return false;
		}
		if (duckdb_connect(database, &connection) != DuckDBSuccess) {
			return false;
		}
		return true;
	}

	unique_ptr<CAPIResult> Query(string query) {
		auto result = make_unique<CAPIResult>();
		result->Query(connection, query);
		return result;
	}

	duckdb_database database = nullptr;
	duckdb_connection connection = nullptr;
};

TEST_CASE("Basic test of C API", "[capi]") {
	CAPITester tester;
	unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	// select scalar value
	result = tester.Query("SELECT CAST(42 AS BIGINT)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ColumnType(0) == DUCKDB_TYPE_BIGINT);
	REQUIRE(result->ColumnData<int64_t>(0)[0] == 42);
	REQUIRE(result->ColumnCount() == 1);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);
	REQUIRE(!result->IsNull(0, 0));
	// out of range fetch
	REQUIRE(result->Fetch<int64_t>(1, 0) == 0);
	REQUIRE(result->Fetch<int64_t>(0, 1) == 0);

	// select scalar NULL
	result = tester.Query("SELECT NULL");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ColumnCount() == 1);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 0);
	REQUIRE(result->IsNull(0, 0));

	// select scalar string
	result = tester.Query("SELECT 'hello'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ColumnCount() == 1);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<string>(0, 0) == "hello");
	REQUIRE(!result->IsNull(0, 0));

	result = tester.Query("SELECT 1=1");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ColumnCount() == 1);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<bool>(0, 0) == true);
	REQUIRE(!result->IsNull(0, 0));

	result = tester.Query("SELECT 1=0");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ColumnCount() == 1);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<bool>(0, 0) == false);
	REQUIRE(!result->IsNull(0, 0));

	result = tester.Query("SELECT i FROM (values (true), (false)) tbl(i) group by i order by i");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ColumnCount() == 1);
	REQUIRE(result->row_count() == 2);
	REQUIRE(result->Fetch<bool>(0, 0) == false);
	REQUIRE(result->Fetch<bool>(0, 1) == true);
	REQUIRE(!result->IsNull(0, 0));

	// multiple insertions
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO test VALUES (11, 22)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO test VALUES (NULL, 21)"));
	result = tester.Query("INSERT INTO test VALUES (13, 22)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->rows_changed() == 1);

	// NULL selection
	result = tester.Query("SELECT a, b FROM test ORDER BY a");
	REQUIRE_NO_FAIL(*result);
	// NULL, 11, 13
	REQUIRE(result->IsNull(0, 0));
	REQUIRE(result->Fetch<int32_t>(0, 1) == 11);
	REQUIRE(result->Fetch<int32_t>(0, 2) == 13);
	// 21, 22, 22
	REQUIRE(result->Fetch<int32_t>(1, 0) == 21);
	REQUIRE(result->Fetch<int32_t>(1, 1) == 22);
	REQUIRE(result->Fetch<int32_t>(1, 2) == 22);

	REQUIRE(result->ColumnName(0) == "a");
	REQUIRE(result->ColumnName(1) == "b");
	REQUIRE(result->ColumnName(2) == "");

	result = tester.Query("UPDATE test SET a = 1 WHERE b=22");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->rows_changed() == 2);

	// several error conditions
	REQUIRE(duckdb_value_is_null(nullptr, 0, 0) == false);
	REQUIRE(duckdb_column_type(nullptr, 0) == DUCKDB_TYPE_INVALID);
	REQUIRE(duckdb_column_count(nullptr) == 0);
	REQUIRE(duckdb_row_count(nullptr) == 0);
	REQUIRE(duckdb_rows_changed(nullptr) == 0);
	REQUIRE(duckdb_result_error(nullptr) == nullptr);
	REQUIRE(duckdb_nullmask_data(nullptr, 0) == nullptr);
	REQUIRE(duckdb_column_data(nullptr, 0) == nullptr);
}

TEST_CASE("Test different types of C API", "[capi]") {
	CAPITester tester;
	unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	// integer columns
	vector<string> types = {"TINYINT",  "SMALLINT",  "INTEGER",  "BIGINT", "HUGEINT",
	                        "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"};
	for (auto &type : types) {
		// create the table and insert values
		REQUIRE_NO_FAIL(tester.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE integers(i " + type + ")"));
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO integers VALUES (1), (NULL)"));

		result = tester.Query("SELECT * FROM integers ORDER BY i");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->IsNull(0, 0));
		REQUIRE(result->Fetch<int8_t>(0, 0) == 0);
		REQUIRE(result->Fetch<int16_t>(0, 0) == 0);
		REQUIRE(result->Fetch<int32_t>(0, 0) == 0);
		REQUIRE(result->Fetch<int64_t>(0, 0) == 0);
		REQUIRE(result->Fetch<uint8_t>(0, 0) == 0);
		REQUIRE(result->Fetch<uint16_t>(0, 0) == 0);
		REQUIRE(result->Fetch<uint32_t>(0, 0) == 0);
		REQUIRE(result->Fetch<uint64_t>(0, 0) == 0);
		REQUIRE(duckdb_hugeint_to_double(result->Fetch<duckdb_hugeint>(0, 0)) == 0);
		REQUIRE(result->Fetch<string>(0, 0) == "");
		REQUIRE(ApproxEqual(result->Fetch<float>(0, 0), 0.0f));
		REQUIRE(ApproxEqual(result->Fetch<double>(0, 0), 0.0));

		REQUIRE(!result->IsNull(0, 1));
		REQUIRE(result->Fetch<int8_t>(0, 1) == 1);
		REQUIRE(result->Fetch<int16_t>(0, 1) == 1);
		REQUIRE(result->Fetch<int32_t>(0, 1) == 1);
		REQUIRE(result->Fetch<int64_t>(0, 1) == 1);
		REQUIRE(result->Fetch<uint8_t>(0, 1) == 1);
		REQUIRE(result->Fetch<uint16_t>(0, 1) == 1);
		REQUIRE(result->Fetch<uint32_t>(0, 1) == 1);
		REQUIRE(result->Fetch<uint64_t>(0, 1) == 1);
		REQUIRE(duckdb_hugeint_to_double(result->Fetch<duckdb_hugeint>(0, 1)) == 1);
		REQUIRE(ApproxEqual(result->Fetch<float>(0, 1), 1.0f));
		REQUIRE(ApproxEqual(result->Fetch<double>(0, 1), 1.0));
		REQUIRE(result->Fetch<string>(0, 1) == "1");

		REQUIRE_NO_FAIL(tester.Query("ROLLBACK"));
	}
	// real/double columns
	types = {"REAL", "DOUBLE"};
	for (auto &type : types) {
		// create the table and insert values
		REQUIRE_NO_FAIL(tester.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE doubles(i " + type + ")"));
		REQUIRE_NO_FAIL(tester.Query("INSERT INTO doubles VALUES (1), (NULL)"));

		result = tester.Query("SELECT * FROM doubles ORDER BY i");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->IsNull(0, 0));
		REQUIRE(result->Fetch<int8_t>(0, 0) == 0);
		REQUIRE(result->Fetch<int16_t>(0, 0) == 0);
		REQUIRE(result->Fetch<int32_t>(0, 0) == 0);
		REQUIRE(result->Fetch<int64_t>(0, 0) == 0);
		REQUIRE(result->Fetch<string>(0, 0) == "");
		REQUIRE(ApproxEqual(result->Fetch<float>(0, 0), 0.0f));
		REQUIRE(ApproxEqual(result->Fetch<double>(0, 0), 0.0));

		REQUIRE(!result->IsNull(0, 1));
		REQUIRE(result->Fetch<int8_t>(0, 1) == 1);
		REQUIRE(result->Fetch<int16_t>(0, 1) == 1);
		REQUIRE(result->Fetch<int32_t>(0, 1) == 1);
		REQUIRE(result->Fetch<int64_t>(0, 1) == 1);
		REQUIRE(ApproxEqual(result->Fetch<float>(0, 1), 1.0f));
		REQUIRE(ApproxEqual(result->Fetch<double>(0, 1), 1.0));

		REQUIRE_NO_FAIL(tester.Query("ROLLBACK"));
	}
	// date columns
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE dates(d DATE)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO dates VALUES ('1992-09-20'), (NULL), ('30000-09-20')"));

	result = tester.Query("SELECT * FROM dates ORDER BY d");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->IsNull(0, 0));
	duckdb_date_struct date = duckdb_from_date(result->Fetch<duckdb_date>(0, 1));
	REQUIRE(date.year == 1992);
	REQUIRE(date.month == 9);
	REQUIRE(date.day == 20);
	REQUIRE(result->Fetch<string>(0, 1) == Value::DATE(1992, 9, 20).ToString());
	date = duckdb_from_date(result->Fetch<duckdb_date>(0, 2));
	REQUIRE(date.year == 30000);
	REQUIRE(date.month == 9);
	REQUIRE(date.day == 20);
	REQUIRE(result->Fetch<string>(0, 2) == Value::DATE(30000, 9, 20).ToString());

	// timestamp columns
	REQUIRE_NO_FAIL(tester.Query(
	    "CREATE TABLE timestamp (sec TIMESTAMP_S, milli TIMESTAMP_MS,micro TIMESTAMP_US, nano TIMESTAMP_NS );"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO timestamp VALUES (NULL,NULL,NULL,NULL )"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO timestamp VALUES ('1992-09-20 12:01:30','1992-09-20 "
	                             "12:01:30','1992-09-20 12:01:30','1992-09-20 12:01:30')"));

	result = tester.Query("SELECT * FROM timestamp ORDER BY sec");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->IsNull(0, 0));
	REQUIRE(result->IsNull(1, 0));
	REQUIRE(result->IsNull(2, 0));
	REQUIRE(result->IsNull(3, 0));
	for (idx_t i = 0; i < 4; i++) {
		duckdb_timestamp_struct stamp = duckdb_from_timestamp(result->Fetch<duckdb_timestamp>(i, 1));
		REQUIRE(stamp.date.year == 1992);
		REQUIRE(stamp.date.month == 9);
		REQUIRE(stamp.date.day == 20);
		REQUIRE(stamp.time.hour == 12);
		REQUIRE(stamp.time.min == 1);
		REQUIRE(stamp.time.sec == 30);
		REQUIRE(stamp.time.micros == 0);
		auto result_string = result->Fetch<string>(i, 1);
		auto timestamp_string = Value::TIMESTAMP(1992, 9, 20, 12, 1, 30, 0).ToString();
		REQUIRE(result->Fetch<string>(i, 1) == timestamp_string);
	}

	// time columns
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE times(d TIME)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO times VALUES ('12:00:30.1234'), (NULL), ('02:30:01')"));

	result = tester.Query("SELECT * FROM times ORDER BY d");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->IsNull(0, 0));
	duckdb_time_struct time_val = duckdb_from_time(result->Fetch<duckdb_time>(0, 1));
	REQUIRE(time_val.hour == 2);
	REQUIRE(time_val.min == 30);
	REQUIRE(time_val.sec == 1);
	REQUIRE(time_val.micros == 0);
	REQUIRE(result->Fetch<string>(0, 1) == Value::TIME(2, 30, 1, 0).ToString());
	time_val = duckdb_from_time(result->Fetch<duckdb_time>(0, 2));
	REQUIRE(time_val.hour == 12);
	REQUIRE(time_val.min == 0);
	REQUIRE(time_val.sec == 30);
	REQUIRE(time_val.micros == 123400);
	REQUIRE(result->Fetch<string>(0, 2) == Value::TIME(12, 0, 30, 123400).ToString());

	// blob columns
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE blobs(b BLOB)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO blobs VALUES ('hello\\x12world'), ('\\x00'), (NULL)"));

	result = tester.Query("SELECT * FROM blobs");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(!result->IsNull(0, 0));
	duckdb_blob blob = result->Fetch<duckdb_blob>(0, 0);
	REQUIRE(blob.size == 11);
	REQUIRE(memcmp(blob.data, "hello\012world", 11));
	REQUIRE(result->Fetch<string>(0, 1) == "\\x00");
	REQUIRE(result->IsNull(0, 2));
	blob = result->Fetch<duckdb_blob>(0, 2);
	REQUIRE(blob.data == nullptr);
	REQUIRE(blob.size == 0);

	// boolean columns
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE booleans(b BOOLEAN)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO booleans VALUES (42 > 60), (42 > 20), (42 > NULL)"));

	result = tester.Query("SELECT * FROM booleans ORDER BY b");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->IsNull(0, 0));
	REQUIRE(!result->Fetch<bool>(0, 0));
	REQUIRE(!result->Fetch<bool>(0, 1));
	REQUIRE(result->Fetch<bool>(0, 2));
	REQUIRE(result->Fetch<string>(0, 2) == "true");
}

TEST_CASE("Test errors in C API", "[capi]") {
	CAPITester tester;
	unique_ptr<CAPIResult> result;

	// cannot open database in random directory
	REQUIRE(!tester.OpenDatabase("/bla/this/directory/should/not/exist/hopefully/awerar333"));
	REQUIRE(tester.OpenDatabase(nullptr));

	// syntax error in query
	REQUIRE_FAIL(tester.Query("SELEC * FROM TABLE"));
	// bind error
	REQUIRE_FAIL(tester.Query("SELECT * FROM TABLE"));

	duckdb_result res;
	duckdb_prepared_statement stmt = nullptr;
	// fail prepare API calls
	REQUIRE(duckdb_prepare(NULL, "SELECT 42", &stmt) == DuckDBError);
	REQUIRE(duckdb_prepare(tester.connection, NULL, &stmt) == DuckDBError);
	REQUIRE(stmt == nullptr);

	REQUIRE(duckdb_prepare(tester.connection, "SELECT * from INVALID_TABLE", &stmt) == DuckDBError);
	REQUIRE(duckdb_prepare_error(nullptr) == nullptr);
	REQUIRE(stmt != nullptr);
	auto err_msg = duckdb_prepare_error(stmt);
	REQUIRE(err_msg != nullptr);
	duckdb_destroy_prepare(&stmt);

	REQUIRE(duckdb_bind_boolean(NULL, 0, true) == DuckDBError);
	REQUIRE(duckdb_execute_prepared(NULL, &res) == DuckDBError);
	duckdb_destroy_prepare(NULL);

	// fail to query arrow
	duckdb_arrow out_arrow;
	REQUIRE(duckdb_query_arrow(tester.connection, "SELECT * from INVALID_TABLE", &out_arrow) == DuckDBError);
	err_msg = duckdb_query_arrow_error(out_arrow);
	REQUIRE(err_msg != nullptr);
	duckdb_free((void *)err_msg);
	duckdb_destroy_arrow(&out_arrow);

	// various edge cases/nullptrs
	REQUIRE(duckdb_query_arrow_schema(out_arrow, nullptr) == DuckDBSuccess);
	REQUIRE(duckdb_query_arrow_array(out_arrow, nullptr) == DuckDBSuccess);
}

TEST_CASE("Test prepared statements in C API", "[capi]") {
	CAPITester tester;
	unique_ptr<CAPIResult> result;
	duckdb_result res;
	duckdb_prepared_statement stmt = nullptr;
	duckdb_state status;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	status = duckdb_prepare(tester.connection, "SELECT CAST($1 AS BIGINT)", &stmt);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(stmt != nullptr);

	status = duckdb_bind_boolean(stmt, 1, 1);
	REQUIRE(status == DuckDBSuccess);
	status = duckdb_bind_boolean(stmt, 2, 1);
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

	REQUIRE(duckdb_bind_float(stmt, 1, NAN) == DuckDBError);
	REQUIRE(duckdb_bind_double(stmt, 1, NAN) == DuckDBError);

	duckdb_bind_varchar(stmt, 1, "44");
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_value_int64(&res, 0, 0) == 44);
	duckdb_destroy_result(&res);

	duckdb_bind_null(stmt, 1);
	status = duckdb_execute_prepared(stmt, &res);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(res.columns[0].nullmask[0] == true);
	duckdb_destroy_result(&res);

	duckdb_destroy_prepare(&stmt);
	// again to make sure it does not crash
	duckdb_destroy_result(&res);
	duckdb_destroy_prepare(&stmt);

	status = duckdb_prepare(tester.connection, "SELECT CAST($1 AS VARCHAR)", &stmt);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(stmt != nullptr);

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
	REQUIRE(duckdb_param_type(stmt, 0) == DUCKDB_TYPE_INTEGER);
	REQUIRE(duckdb_param_type(stmt, 1) == DUCKDB_TYPE_INVALID);

	for (int32_t i = 1; i <= 1000; i++) {
		duckdb_bind_int32(stmt, 1, i);
		status = duckdb_execute_prepared(stmt, nullptr);
		REQUIRE(status == DuckDBSuccess);
	}
	duckdb_destroy_prepare(&stmt);

	status = duckdb_prepare(tester.connection, "SELECT SUM(i)*$1-$2 FROM a", &stmt);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(stmt != nullptr);
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
}

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
	auto msg = duckdb_appender_error(appender);
	REQUIRE(msg != nullptr);
	duckdb_free((void *)msg);
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
	auto err_msg = duckdb_appender_error(appender);
	REQUIRE(err_msg != nullptr);
	duckdb_free((void *)err_msg);

	status = duckdb_append_varchar(appender, "Hello, World");
	REQUIRE(status == DuckDBSuccess);

	// out of cols here
	status = duckdb_append_int32(appender, 42);
	REQUIRE(status == DuckDBError);

	err_msg = duckdb_appender_error(appender);
	REQUIRE(err_msg != nullptr);
	duckdb_free((void *)err_msg);

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

	status = duckdb_appender_begin_row(appender);
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

	status = duckdb_appender_begin_row(nullptr);
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

	auto str = strdup("hello world this is my long string");
	status = duckdb_append_blob(tappender, str, strlen(str));
	free(str);
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
	REQUIRE(blob.size == 34);
	REQUIRE(memcmp(blob.data, "hello world this is my long string", 34) == 0);
	duckdb_free(blob.data);

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
	REQUIRE(result->Fetch<string>(10, 1) == "");

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

TEST_CASE("Test arrow in C API", "[capi]") {
	CAPITester tester;
	unique_ptr<CAPIResult> result;
	duckdb_prepared_statement stmt = nullptr;
	duckdb_arrow arrow_result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	// test rows changed
	{
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE test(a INTEGER)"));
		REQUIRE(duckdb_query_arrow(tester.connection, "INSERT INTO test VALUES (1), (2);", &arrow_result) ==
		        DuckDBSuccess);
		REQUIRE(duckdb_arrow_rows_changed(arrow_result) == 2);
		duckdb_destroy_arrow(&arrow_result);
		REQUIRE_NO_FAIL(tester.Query("drop table test"));
	}

	// test query arrow
	{
		REQUIRE(duckdb_query_arrow(tester.connection, "SELECT 42 AS VALUE", &arrow_result) == DuckDBSuccess);
		REQUIRE(duckdb_arrow_row_count(arrow_result) == 1);
		REQUIRE(duckdb_arrow_column_count(arrow_result) == 1);
		REQUIRE(duckdb_arrow_rows_changed(arrow_result) == 0);

		// query schema
		ArrowSchema *arrow_schema = new ArrowSchema();
		REQUIRE(duckdb_query_arrow_schema(arrow_result, (duckdb_arrow_schema *)&arrow_schema) == DuckDBSuccess);
		REQUIRE(string(arrow_schema->name) == "duckdb_query_result");
		// User need to release the data themselves
		arrow_schema->release(arrow_schema);
		delete arrow_schema;

		// query array data
		ArrowArray *arrow_array = new ArrowArray();
		REQUIRE(duckdb_query_arrow_array(arrow_result, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
		REQUIRE(arrow_array->length == 1);
		arrow_array->release(arrow_array);
		delete arrow_array;

		duckdb_arrow_array null_array = nullptr;
		REQUIRE(duckdb_query_arrow_array(arrow_result, &null_array) == DuckDBSuccess);
		REQUIRE(null_array == nullptr);

		// destroy result
		duckdb_destroy_arrow(&arrow_result);
	}

	// test multiple chunks
	{
		// create table that consists of multiple chunks
		REQUIRE_NO_FAIL(tester.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(tester.Query("CREATE TABLE test(a INTEGER)"));
		for (size_t i = 0; i < 500; i++) {
			REQUIRE_NO_FAIL(
			    tester.Query("INSERT INTO test VALUES (1); INSERT INTO test VALUES (2); INSERT INTO test VALUES "
			                 "(3); INSERT INTO test VALUES (4); INSERT INTO test VALUES (5);"));
		}
		REQUIRE_NO_FAIL(tester.Query("COMMIT"));

		REQUIRE(duckdb_query_arrow(tester.connection, "SELECT CAST(a AS INTEGER) AS a FROM test ORDER BY a",
		                           &arrow_result) == DuckDBSuccess);

		ArrowSchema *arrow_schema = new ArrowSchema();
		REQUIRE(duckdb_query_arrow_schema(arrow_result, (duckdb_arrow_schema *)&arrow_schema) == DuckDBSuccess);
		REQUIRE(arrow_schema->release != nullptr);
		arrow_schema->release(arrow_schema);
		delete arrow_schema;

		int total_count = 0;
		while (true) {
			ArrowArray *arrow_array = new ArrowArray();
			REQUIRE(duckdb_query_arrow_array(arrow_result, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
			if (arrow_array->length == 0) {
				delete arrow_array;
				REQUIRE(total_count == 2500);
				break;
			}
			REQUIRE(arrow_array->length > 0);
			total_count += arrow_array->length;
			arrow_array->release(arrow_array);
			delete arrow_array;
		}
		duckdb_destroy_arrow(&arrow_result);
		REQUIRE_NO_FAIL(tester.Query("drop table test"));
	}

	// test prepare query arrow
	{
		REQUIRE(duckdb_prepare(tester.connection, "SELECT CAST($1 AS BIGINT)", &stmt) == DuckDBSuccess);
		REQUIRE(stmt != nullptr);
		REQUIRE(duckdb_bind_int64(stmt, 1, 42) == DuckDBSuccess);
		REQUIRE(duckdb_execute_prepared_arrow(stmt, nullptr) == DuckDBError);
		REQUIRE(duckdb_execute_prepared_arrow(stmt, &arrow_result) == DuckDBSuccess);

		ArrowSchema *arrow_schema = new ArrowSchema();
		REQUIRE(duckdb_query_arrow_schema(arrow_result, (duckdb_arrow_schema *)&arrow_schema) == DuckDBSuccess);
		REQUIRE(string(arrow_schema->format) == "+s");
		arrow_schema->release(arrow_schema);
		delete arrow_schema;

		ArrowArray *arrow_array = new ArrowArray();
		REQUIRE(duckdb_query_arrow_array(arrow_result, (duckdb_arrow_array *)&arrow_array) == DuckDBSuccess);
		REQUIRE(arrow_array->length == 1);
		arrow_array->release(arrow_array);
		delete arrow_array;

		duckdb_destroy_arrow(&arrow_result);
		duckdb_destroy_prepare(&stmt);
	}
}

TEST_CASE("Test C API config", "[capi]") {
	duckdb_database db = nullptr;
	duckdb_connection con = nullptr;
	duckdb_config config = nullptr;
	duckdb_result result;

	// enumerate config options
	auto config_count = duckdb_config_count();
	for (size_t i = 0; i < config_count; i++) {
		const char *name = nullptr;
		const char *description = nullptr;
		duckdb_get_config_flag(i, &name, &description);
		REQUIRE(strlen(name) > 0);
		REQUIRE(strlen(description) > 0);
	}

	// test config creation
	REQUIRE(duckdb_create_config(&config) == DuckDBSuccess);
	REQUIRE(duckdb_set_config(config, "access_mode", "invalid_access_mode") == DuckDBError);
	REQUIRE(duckdb_set_config(config, "access_mode", "read_only") == DuckDBSuccess);
	REQUIRE(duckdb_set_config(config, "aaaa_invalidoption", "read_only") == DuckDBError);

	auto dbdir = TestCreatePath("capi_read_only_db");

	// open the database & connection
	// cannot open an in-memory database in read-only mode
	char *error = nullptr;
	REQUIRE(duckdb_open_ext(":memory:", &db, config, &error) == DuckDBError);
	REQUIRE(strlen(error) > 0);
	duckdb_free(error);
	// now without the error
	REQUIRE(duckdb_open_ext(":memory:", &db, config, nullptr) == DuckDBError);
	// cannot open a database that does not exist
	REQUIRE(duckdb_open_ext(dbdir.c_str(), &db, config, &error) == DuckDBError);
	REQUIRE(strlen(error) > 0);
	duckdb_free(error);
	// we can create the database and add some tables
	{
		DuckDB cppdb(dbdir);
		Connection cppcon(cppdb);
		cppcon.Query("CREATE TABLE integers(i INTEGER)");
		cppcon.Query("INSERT INTO integers VALUES (42)");
	}

	// now we can connect
	REQUIRE(duckdb_open_ext(dbdir.c_str(), &db, config, &error) == DuckDBSuccess);

	// we can destroy the config right after duckdb_open
	duckdb_destroy_config(&config);
	// we can spam this
	duckdb_destroy_config(&config);
	duckdb_destroy_config(&config);

	REQUIRE(duckdb_connect(db, nullptr) == DuckDBError);
	REQUIRE(duckdb_connect(nullptr, &con) == DuckDBError);
	REQUIRE(duckdb_connect(db, &con) == DuckDBSuccess);

	// we can query
	REQUIRE(duckdb_query(con, "SELECT 42::INT", &result) == DuckDBSuccess);
	REQUIRE(duckdb_value_int32(&result, 0, 0) == 42);
	duckdb_destroy_result(&result);
	REQUIRE(duckdb_query(con, "SELECT i::INT FROM integers", &result) == DuckDBSuccess);
	REQUIRE(duckdb_value_int32(&result, 0, 0) == 42);
	duckdb_destroy_result(&result);

	// but we cannot create new tables
	REQUIRE(duckdb_query(con, "CREATE TABLE new_table(i INTEGER)", nullptr) == DuckDBError);

	duckdb_disconnect(&con);
	duckdb_close(&db);

	// api abuse
	REQUIRE(duckdb_create_config(nullptr) == DuckDBError);
	REQUIRE(duckdb_get_config_flag(9999999, nullptr, nullptr) == DuckDBError);
	REQUIRE(duckdb_set_config(nullptr, nullptr, nullptr) == DuckDBError);
	REQUIRE(duckdb_create_config(nullptr) == DuckDBError);
	duckdb_destroy_config(nullptr);
	duckdb_destroy_config(nullptr);
}

TEST_CASE("Issue #2058: Cleanup after execution of invalid SQL statement causes segmentation fault", "[capi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;
	duckdb_result result_count;

	REQUIRE(duckdb_open(NULL, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "CREATE TABLE integers(i INTEGER, j INTEGER);", NULL) != DuckDBError);
	REQUIRE((duckdb_query(con, "SELECT count(*) FROM integers;", &result_count) != DuckDBError));

	duckdb_destroy_result(&result_count);

	REQUIRE(duckdb_query(con, "non valid SQL", &result) == DuckDBError);

	duckdb_destroy_result(&result); // segmentation failure happens here

	duckdb_disconnect(&con);
	duckdb_close(&db);
}

TEST_CASE("Test C API examples from the website", "[capi]") {
	// NOTE: if any of these break and need to be changed, the website also needs to be updated!
	SECTION("connect") {
		duckdb_database db;
		duckdb_connection con;

		if (duckdb_open(NULL, &db) == DuckDBError) {
			// handle error
		}
		if (duckdb_connect(db, &con) == DuckDBError) {
			// handle error
		}

		// run queries...

		// cleanup
		duckdb_disconnect(&con);
		duckdb_close(&db);
	}
	SECTION("config") {
		duckdb_database db;
		duckdb_config config;

		// create the configuration object
		if (duckdb_create_config(&config) == DuckDBError) {
			REQUIRE(1 == 0);
		}
		// set some configuration options
		duckdb_set_config(config, "access_mode", "READ_WRITE");
		duckdb_set_config(config, "threads", "8");
		duckdb_set_config(config, "max_memory", "8GB");
		duckdb_set_config(config, "default_order", "DESC");

		// open the database using the configuration
		if (duckdb_open_ext(NULL, &db, config, NULL) == DuckDBError) {
			REQUIRE(1 == 0);
		}
		// cleanup the configuration object
		duckdb_destroy_config(&config);

		// run queries...

		// cleanup
		duckdb_close(&db);
	}
	SECTION("query") {
		duckdb_database db;
		duckdb_connection con;
		duckdb_state state;
		duckdb_result result;

		duckdb_open(NULL, &db);
		duckdb_connect(db, &con);

		// create a table
		state = duckdb_query(con, "CREATE TABLE integers(i INTEGER, j INTEGER);", NULL);
		if (state == DuckDBError) {
			REQUIRE(1 == 0);
		}
		// insert three rows into the table
		state = duckdb_query(con, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", NULL);
		if (state == DuckDBError) {
			REQUIRE(1 == 0);
		}
		// query rows again
		state = duckdb_query(con, "SELECT * FROM integers", &result);
		if (state == DuckDBError) {
			REQUIRE(1 == 0);
		}
		// handle the result
		idx_t row_count = duckdb_row_count(&result);
		idx_t column_count = duckdb_column_count(&result);
		for (idx_t row = 0; row < row_count; row++) {
			for (idx_t col = 0; col < column_count; col++) {
				// if (col > 0) printf(",");
				auto str_val = duckdb_value_varchar(&result, col, row);
				// printf("%s", str_val);
				REQUIRE(1 == 1);
				duckdb_free(str_val);
			}
			//	printf("\n");
		}

		int32_t *i_data = (int32_t *)duckdb_column_data(&result, 0);
		int32_t *j_data = (int32_t *)duckdb_column_data(&result, 1);
		bool *i_mask = duckdb_nullmask_data(&result, 0);
		bool *j_mask = duckdb_nullmask_data(&result, 1);
		for (idx_t row = 0; row < row_count; row++) {
			if (i_mask[row]) {
				// printf("NULL");
			} else {
				REQUIRE(i_data[row] > 0);
				// printf("%d", i_data[row]);
			}
			// printf(",");
			if (j_mask[row]) {
				// printf("NULL");
			} else {
				REQUIRE(j_data[row] > 0);
				// printf("%d", j_data[row]);
			}
			// printf("\n");
		}

		// destroy the result after we are done with it
		duckdb_destroy_result(&result);
		duckdb_disconnect(&con);
		duckdb_close(&db);
	}
	SECTION("prepared") {
		duckdb_database db;
		duckdb_connection con;
		duckdb_open(NULL, &db);
		duckdb_connect(db, &con);
		duckdb_query(con, "CREATE TABLE integers(i INTEGER, j INTEGER)", NULL);

		duckdb_prepared_statement stmt;
		duckdb_result result;
		if (duckdb_prepare(con, "INSERT INTO integers VALUES ($1, $2)", &stmt) == DuckDBError) {
			REQUIRE(1 == 0);
		}

		duckdb_bind_int32(stmt, 1, 42); // the parameter index starts counting at 1!
		duckdb_bind_int32(stmt, 2, 43);
		// NULL as second parameter means no result set is requested
		duckdb_execute_prepared(stmt, NULL);
		duckdb_destroy_prepare(&stmt);

		// we can also query result sets using prepared statements
		if (duckdb_prepare(con, "SELECT * FROM integers WHERE i = ?", &stmt) == DuckDBError) {
			REQUIRE(1 == 0);
		}
		duckdb_bind_int32(stmt, 1, 42);
		duckdb_execute_prepared(stmt, &result);

		// do something with result

		// clean up
		duckdb_destroy_result(&result);
		duckdb_destroy_prepare(&stmt);

		duckdb_disconnect(&con);
		duckdb_close(&db);
	}
	SECTION("appender") {
		duckdb_database db;
		duckdb_connection con;
		duckdb_open(NULL, &db);
		duckdb_connect(db, &con);
		duckdb_query(con, "CREATE TABLE people(id INTEGER, name VARCHAR)", NULL);

		duckdb_appender appender;
		if (duckdb_appender_create(con, NULL, "people", &appender) == DuckDBError) {
			REQUIRE(1 == 0);
		}
		duckdb_append_int32(appender, 1);
		duckdb_append_varchar(appender, "Mark");
		duckdb_appender_end_row(appender);

		duckdb_append_int32(appender, 2);
		duckdb_append_varchar(appender, "Hannes");
		duckdb_appender_end_row(appender);

		duckdb_appender_destroy(&appender);

		duckdb_result result;
		duckdb_query(con, "SELECT * FROM people", &result);
		REQUIRE(duckdb_value_int32(&result, 0, 0) == 1);
		REQUIRE(duckdb_value_int32(&result, 0, 1) == 2);
		REQUIRE(string(duckdb_value_varchar_internal(&result, 1, 0)) == "Mark");
		REQUIRE(string(duckdb_value_varchar_internal(&result, 1, 1)) == "Hannes");

		duckdb_destroy_result(&result);

		duckdb_disconnect(&con);
		duckdb_close(&db);
	}
}
