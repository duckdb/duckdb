#include "catch.hpp"
#include "duckdb.h"
#include "test_helpers.hpp"
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

	idx_t column_count() {
		return result.column_count;
	}

	idx_t row_count() {
		return result.row_count;
	}

	template <class T> T Fetch(idx_t col, idx_t row) {
		throw NotImplementedException("Unimplemented type for fetch");
	}

	bool IsNull(idx_t col, idx_t row) {
		return result.columns[col].nullmask[row];
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

template <> bool CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_boolean(&result, col, row);
}

template <> int8_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int8(&result, col, row);
}

template <> int16_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int16(&result, col, row);
}

template <> int32_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int32(&result, col, row);
}

template <> int64_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int64(&result, col, row);
}

template <> float CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_float(&result, col, row);
}

template <> double CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_double(&result, col, row);
}

template <> duckdb_date CAPIResult::Fetch(idx_t col, idx_t row) {
	auto data = (duckdb_date *)result.columns[col].data;
	return data[row];
}

template <> duckdb_timestamp CAPIResult::Fetch(idx_t col, idx_t row) {
	auto data = (duckdb_timestamp *)result.columns[col].data;
	return data[row];
}

template <> string CAPIResult::Fetch(idx_t col, idx_t row) {
	auto value = duckdb_value_varchar(&result, col, row);
	string strval = string(value);
	free((void *)value);
	return strval;
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
	REQUIRE(result->column_count() == 1);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);
	REQUIRE(!result->IsNull(0, 0));

	// select scalar NULL
	result = tester.Query("SELECT NULL");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->column_count() == 1);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 0);
	REQUIRE(result->IsNull(0, 0));

	// select scalar string
	result = tester.Query("SELECT 'hello'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->column_count() == 1);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<string>(0, 0) == "hello");
	REQUIRE(!result->IsNull(0, 0));

	// multiple insertions
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO test VALUES (11, 22)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO test VALUES (NULL, 21)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO test VALUES (13, 22)"));

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
}

TEST_CASE("Test different types of C API", "[capi]") {
	CAPITester tester;
	unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	// integer columns
	vector<string> types = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT"};
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
		REQUIRE(ApproxEqual(result->Fetch<float>(0, 0), 0.0f));
		REQUIRE(ApproxEqual(result->Fetch<double>(0, 0), 0.0));

		REQUIRE(!result->IsNull(0, 1));
		REQUIRE(result->Fetch<int8_t>(0, 1) == 1);
		REQUIRE(result->Fetch<int16_t>(0, 1) == 1);
		REQUIRE(result->Fetch<int32_t>(0, 1) == 1);
		REQUIRE(result->Fetch<int64_t>(0, 1) == 1);
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
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO dates VALUES ('1992-09-20'), (NULL), ('1000000-09-20')"));

	result = tester.Query("SELECT * FROM dates ORDER BY d");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->IsNull(0, 0));
	duckdb_date date = result->Fetch<duckdb_date>(0, 1);
	REQUIRE(date.year == 1992);
	REQUIRE(date.month == 9);
	REQUIRE(date.day == 20);
	REQUIRE(result->Fetch<string>(0, 1) == Value::DATE(1992, 9, 20).ToString(SQLType::DATE));
	date = result->Fetch<duckdb_date>(0, 2);
	REQUIRE(date.year == 1000000);
	REQUIRE(date.month == 9);
	REQUIRE(date.day == 20);
	REQUIRE(result->Fetch<string>(0, 2) == Value::DATE(1000000, 9, 20).ToString(SQLType::DATE));

	// timestamp columns
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE timestamps(t TIMESTAMP)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO timestamps VALUES ('1992-09-20 12:01:30'), (NULL)"));

	result = tester.Query("SELECT * FROM timestamps ORDER BY t");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->IsNull(0, 0));
	duckdb_timestamp stamp = result->Fetch<duckdb_timestamp>(0, 1);
	REQUIRE(stamp.date.year == 1992);
	REQUIRE(stamp.date.month == 9);
	REQUIRE(stamp.date.day == 20);
	REQUIRE(stamp.time.hour == 12);
	REQUIRE(stamp.time.min == 1);
	REQUIRE(stamp.time.sec == 30);
	REQUIRE(stamp.time.msec == 0);
	REQUIRE(result->Fetch<string>(0, 1) == Value::TIMESTAMP(1992, 9, 20, 12, 1, 30, 0).ToString(SQLType::TIMESTAMP));

	// boolean columns
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE booleans(b BOOLEAN)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO booleans VALUES (42 > 60), (42 > 20), (42 > NULL)"));

	result = tester.Query("SELECT * FROM booleans ORDER BY b");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->IsNull(0, 0));
	REQUIRE(!result->Fetch<bool>(0, 0));
	REQUIRE(!result->Fetch<bool>(0, 1));
	REQUIRE(result->Fetch<bool>(0, 2));
	REQUIRE(result->Fetch<string>(0, 2) == Value::BOOLEAN(true).ToString());
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
	REQUIRE(duckdb_bind_boolean(NULL, 0, true) == DuckDBError);
	REQUIRE(duckdb_execute_prepared(NULL, &res) == DuckDBError);
	duckdb_destroy_prepare(NULL);
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

	status = duckdb_query(tester.connection, "CREATE TABLE a (i INTEGER)", NULL);
	REQUIRE(status == DuckDBSuccess);

	status = duckdb_prepare(tester.connection, "INSERT INTO a VALUES (?)", &stmt);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(stmt != nullptr);
	idx_t nparams;
	REQUIRE(duckdb_nparams(stmt, &nparams) == DuckDBSuccess);
	REQUIRE(nparams == 1);

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
}
