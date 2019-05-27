#include "catch.hpp"
#include "duckdb.h"
#include "test_helpers.hpp"
#include "common/exception.hpp"

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

	count_t column_count() {
		return result.column_count;
	}

	count_t row_count() {
		return result.row_count;
	}

	template <class T> T Fetch(index_t col, index_t row) {
		throw NotImplementedException("Unimplemented type for fetch");
	}

	bool IsNull(index_t col, index_t row) {
		return result.columns[col].nullmask[row];
	}

public:
	bool success = false;

private:
	duckdb_result result;
};

template <> bool CAPIResult::Fetch(index_t col, index_t row) {
	return duckdb_value_boolean(&result, col, row);
}

template <> int8_t CAPIResult::Fetch(index_t col, index_t row) {
	return duckdb_value_int8(&result, col, row);
}

template <> int16_t CAPIResult::Fetch(index_t col, index_t row) {
	return duckdb_value_int16(&result, col, row);
}

template <> int32_t CAPIResult::Fetch(index_t col, index_t row) {
	return duckdb_value_int32(&result, col, row);
}

template <> int64_t CAPIResult::Fetch(index_t col, index_t row) {
	return duckdb_value_int64(&result, col, row);
}

template <> float CAPIResult::Fetch(index_t col, index_t row) {
	return duckdb_value_float(&result, col, row);
}

template <> double CAPIResult::Fetch(index_t col, index_t row) {
	return duckdb_value_double(&result, col, row);
}

template <> duckdb_date CAPIResult::Fetch(index_t col, index_t row) {
	auto data = (duckdb_date *)result.columns[col].data;
	return data[row];
}

template <> string CAPIResult::Fetch(index_t col, index_t row) {
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

private:
	duckdb_database database = nullptr;
	duckdb_connection connection = nullptr;
};

TEST_CASE("Basic test of C API", "[capi]") {
	CAPITester tester;
	unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	// select scalar value
	REQUIRE_NO_FAIL(result = tester.Query("SELECT CAST(42 AS BIGINT)"));
	REQUIRE(result->column_count() == 1);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);
	REQUIRE(!result->IsNull(0, 0));

	// select scalar NULL
	REQUIRE_NO_FAIL(result = tester.Query("SELECT NULL"));
	REQUIRE(result->column_count() == 1);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 0);
	REQUIRE(result->IsNull(0, 0));

	// select scalar string
	REQUIRE_NO_FAIL(result = tester.Query("SELECT 'hello'"));
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
	REQUIRE_NO_FAIL(result = tester.Query("SELECT a, b FROM test ORDER BY a"));
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

		REQUIRE_NO_FAIL(result = tester.Query("SELECT * FROM integers ORDER BY i"));
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

		REQUIRE_NO_FAIL(result = tester.Query("SELECT * FROM doubles ORDER BY i"));
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
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO dates VALUES ('1992-09-20'), (NULL)"));

	REQUIRE_NO_FAIL(result = tester.Query("SELECT * FROM dates ORDER BY d"));
	REQUIRE(result->IsNull(0, 0));
	duckdb_date date = result->Fetch<duckdb_date>(0, 1);
	REQUIRE(date.year == 1992);
	REQUIRE(date.month == 9);
	REQUIRE(date.day == 20);

	// boolean columns
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE booleans(b BOOLEAN)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO booleans VALUES (42 > 60), (42 > 20), (42 > NULL)"));

	REQUIRE_NO_FAIL(result = tester.Query("SELECT * FROM booleans ORDER BY b"));
	REQUIRE(result->IsNull(0, 0));
	REQUIRE(!result->Fetch<bool>(0, 0));
	REQUIRE(!result->Fetch<bool>(0, 1));
	REQUIRE(result->Fetch<bool>(0, 2));
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
}
