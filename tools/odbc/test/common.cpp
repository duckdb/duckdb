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

void METADATA_CHECK(SQLRETURN &ret, SQLHSTMT hstmt, SQLUSMALLINT col_num, const char *expected_col_name,
                           SQLSMALLINT expected_col_name_len, SQLSMALLINT expected_col_data_type,
                           SQLULEN expected_col_size, SQLSMALLINT expected_col_decimal_digits,
                           SQLSMALLINT expected_col_nullable) {
	SQLCHAR col_name[256];
	SQLSMALLINT col_name_len;
	SQLSMALLINT col_type;
	SQLULEN col_size;
	SQLSMALLINT col_decimal_digits;
	SQLSMALLINT col_nullable;

	ret = SQLDescribeCol(hstmt, col_num, col_name, sizeof(col_name), &col_name_len, &col_type, &col_size,
	                     &col_decimal_digits, &col_nullable);

	if (sizeof(col_name) > 0) {
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

void CONNECT_TO_DATABASE(SQLRETURN &ret, SQLHANDLE &env, SQLHANDLE &dbc) {
	auto dsn = "DuckDB";

	ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)(uintptr_t)SQL_OV_ODBC3, 0);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLAllocHandle (DBC)");

	ret = SQLConnect(dbc, (SQLCHAR *)dsn, SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS);
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

} // namespace odbc_test
