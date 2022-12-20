#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>

using namespace std;

static void ODBC_CHECK(SQLRETURN ret, SQLSMALLINT tpe, SQLHANDLE hnd, const char *func) {
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

TEST_CASE("Basic ODBC usage", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	auto dsn = "DuckDB";

	ret = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &env);
	REQUIRE(ret == SQL_SUCCESS);

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)(uintptr_t)SQL_OV_ODBC3, 0);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLAllocHandle (DBC)");

	ret = SQLConnect(dbc, (SQLCHAR *)dsn, SQL_NTS, NULL, SQL_NTS, NULL, SQL_NTS);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLConnect");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLAllocHandle (STMT)");

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	ODBC_CHECK(ret, SQL_HANDLE_STMT, stmt, "SQLFreeHandle (STMT)");

	ret = SQLDisconnect(dbc);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	ODBC_CHECK(ret, SQL_HANDLE_DBC, dbc, "SQLFreeHandle (DBC)");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLFreeHandle (ENV)");
}
