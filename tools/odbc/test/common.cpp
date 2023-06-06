#define CATCH_CONFIG_MAIN
#include "common.h"

using namespace std;
namespace odbc_test {

void ODBC_CHECK(SQLRETURN ret, SQLSMALLINT tpe, SQLHANDLE hnd, const char *func) {
	switch (ret) {
	case SQL_SUCCESS:
		REQUIRE(1 == 1);
		return;
	case SQL_SUCCESS_WITH_INFO:
		fprintf(stderr, "%s: Error: Success with info\n", func);
		break;
	case SQL_ERROR:
		fprintf(stderr, "%s: Error: Error\n", func);
		break;
	case SQL_NO_DATA:
		fprintf(stderr, "%s: Error: no data\n", func);
		break;
	case SQL_INVALID_HANDLE:
		fprintf(stderr, "%s: Error: invalid handle\n", func);
		break;
	default:
		fprintf(stderr, "%s: Unexpected return value\n", func);
		break;
	}
	REQUIRE(ret == SQL_SUCCESS);
}

void ACCESS_DIAGNOSTIC(string &state, string &message, SQLHANDLE handle, SQLRETURN &ret, SQLSMALLINT handle_type) {
	SQLCHAR sqlstate[6];
	SQLINTEGER native_error;
	SQLCHAR message_text[256];
	SQLSMALLINT text_length;
	SQLSMALLINT recnum = 0;

	ret = SQL_SUCCESS;
	while (SQL_SUCCEEDED(ret)) {
		recnum++;
		ret = SQLGetDiagRec(handle_type, handle, recnum, sqlstate, &native_error, message_text, sizeof(message_text),
		                    &text_length);
		if (SQL_SUCCEEDED(ret)) {
			state = (const char *)sqlstate;
			message = (const char *)message_text;
		}
	}

	if (ret != SQL_NO_DATA) {
		ODBC_CHECK(ret, handle_type, handle, "SQLGetDiagRec");
	}
}

void DATA_CHECK(HSTMT hstmt, SQLSMALLINT col_num, const char *expected_content) {
	SQLCHAR content[256];
	SQLLEN content_len;

	SQLRETURN ret = SQLGetData(hstmt, col_num, SQL_C_CHAR, content, sizeof(content), &content_len);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLGetData");
	if (content_len == SQL_NULL_DATA) {
		REQUIRE(expected_content == nullptr);
		return;
	}
	REQUIRE(!::strcmp((const char *)content, expected_content));
}

void METADATA_CHECK(HSTMT hstmt, SQLUSMALLINT col_num, const char *expected_col_name, SQLSMALLINT expected_col_name_len,
                    SQLSMALLINT expected_col_data_type, SQLULEN expected_col_size,
                    SQLSMALLINT expected_col_decimal_digits, SQLSMALLINT expected_col_nullable) {
	SQLCHAR col_name[256];
	SQLSMALLINT col_name_len;
	SQLSMALLINT col_type;
	SQLULEN col_size;
	SQLSMALLINT col_decimal_digits;
	SQLSMALLINT col_nullable;

	SQLRETURN ret = SQLDescribeCol(hstmt, col_num, col_name, sizeof(col_name), &col_name_len, &col_type, &col_size,
	                               &col_decimal_digits, &col_nullable);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLDescribeCol");

	if (sizeof(expected_col_name) > 0) {
		REQUIRE(!::strcmp((const char *)col_name, expected_col_name));
	}
	if (col_name_len) {
		REQUIRE(col_name_len == expected_col_name_len);
	}
	if (col_type) {
		REQUIRE(col_type == expected_col_data_type);
	}
	if (col_size) {
		REQUIRE(col_size == expected_col_size);
	}
	if (col_decimal_digits) {
		REQUIRE(col_decimal_digits == expected_col_decimal_digits);
	}
	if (col_nullable) {
		REQUIRE(col_nullable == expected_col_nullable);
	}
}

void DRIVER_CONNECT_TO_DATABASE(SQLRETURN &ret, SQLHANDLE &env, SQLHANDLE &dbc, const string &extra_params) {
	string dsn;
	string default_dsn = "duckdbmemory";
	SQLCHAR str[1024];
	SQLSMALLINT strl;
	auto tmp = getenv("COMMON_CONNECTION_STRING_FOR_REGRESSION_TEST");
	string envvar = tmp ? tmp : "";

	if (!envvar.empty()) {
		if (!extra_params.empty()) {
			dsn = "DSN=" + default_dsn + ";" + extra_params + ";" + envvar + ";" + extra_params;
		} else {
			dsn = "DSN=" + default_dsn + ";" + envvar;
		}
	} else {
		if (!extra_params.empty()) {
			dsn = "DSN=" + default_dsn + ";" + extra_params;
		} else {
			dsn = "DSN=" + default_dsn;
		}
	}

	ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)(uintptr_t)SQL_OV_ODBC3, 0);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLAllocHandle (DBC)");

	ret = SQLDriverConnect(dbc, nullptr, (SQLCHAR *)dsn.c_str(), SQL_NTS, str, sizeof(str), &strl, SQL_DRIVER_COMPLETE);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLDriverConnect");
}

void CONNECT_TO_DATABASE(SQLRETURN &ret, SQLHANDLE &env, SQLHANDLE &dbc) {
	string dsn = "DuckDB";

	ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)(uintptr_t)SQL_OV_ODBC3, 0);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLAllocHandle (DBC)");

	ret = SQLConnect(dbc, (SQLCHAR *)dsn.c_str(), SQL_NTS, nullptr, 0, nullptr, 0);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLConnect");
}

void DISCONNECT_FROM_DATABASE(SQLRETURN &ret, SQLHANDLE &dbc, SQLHANDLE &env) {
	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLFreeEnv");

	ret = SQLDisconnect(dbc);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLFreeHandle (DBC)");
}

void EXEC_SQL(HSTMT hstmt, string query) {
	SQLRETURN ret;

	ret = SQLExecDirect(hstmt, (SQLCHAR *)query.c_str(), SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, hstmt, "SQLExecDirect");
}

void INITIALIZE_DATABASE(HSTMT hstmt) {
	EXEC_SQL(hstmt, "CREATE TABLE test_table_1 (id integer PRIMARY KEY, t varchar(20));");
	EXEC_SQL(hstmt, "INSERT INTO test_table_1 VALUES (1, 'foo');");
	EXEC_SQL(hstmt, "INSERT INTO test_table_1 VALUES (2, 'bar');");
	EXEC_SQL(hstmt, "INSERT INTO test_table_1 VALUES (3, 'foobar');");

	EXEC_SQL(hstmt, "CREATE TABLE bool_table (id integer, t varchar(5), b boolean);");
	EXEC_SQL(hstmt, "INSERT INTO bool_table VALUES (1, 'yeah', true);");
	EXEC_SQL(hstmt, "INSERT INTO bool_table VALUES (2, 'yes', true);");
	EXEC_SQL(hstmt, "INSERT INTO bool_table VALUES (3, 'true', true);");
	EXEC_SQL(hstmt, "INSERT INTO bool_table VALUES (4, 'false', false)");
	EXEC_SQL(hstmt, "INSERT INTO bool_table VALUES (5, 'not', false);");

	EXEC_SQL(hstmt, "CREATE TABLE byte_table (id integer, t blob);");
	EXEC_SQL(hstmt, "INSERT INTO byte_table VALUES (1, '\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x10'::blob);");
	EXEC_SQL(hstmt, "INSERT INTO byte_table VALUES (2, 'bar');");
	EXEC_SQL(hstmt, "INSERT INTO byte_table VALUES (3, 'foobar');");
	EXEC_SQL(hstmt, "INSERT INTO byte_table VALUES (4, 'foo');");
	EXEC_SQL(hstmt, "INSERT INTO byte_table VALUES (5, 'barf');");

	EXEC_SQL(hstmt, "CREATE TABLE interval_table(id integer, iv interval, d varchar(100));");
	EXEC_SQL(hstmt, "INSERT INTO interval_table VALUES (1, '1 day', 'one day');");
	EXEC_SQL(hstmt, "INSERT INTO interval_table VALUES (2, '10 seconds', 'ten secs');");
	EXEC_SQL(hstmt, "INSERT INTO interval_table VALUES (3, '100 years', 'hundred years');");

	EXEC_SQL(hstmt, "CREATE VIEW test_view AS SELECT * FROM test_table_1;");

	EXEC_SQL(hstmt, "CREATE TABLE lo_test_table (id int4, large_data blob);");
}

} // namespace odbc_test
