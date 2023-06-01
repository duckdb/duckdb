#include "../common.h"

using namespace odbc_test;

TEST_CASE("Basic ODBC usage", "[odbc]") {
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	auto dsn = "DuckDB";

	ret = SQLAllocHandle(SQL_HANDLE_ENV, nullptr, &env);
	REQUIRE(ret == SQL_SUCCESS);

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)(uintptr_t)SQL_OV_ODBC3, 0);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	ODBC_CHECK(ret, SQL_HANDLE_ENV, env, "SQLAllocHandle (DBC)");

	ret = SQLConnect(dbc, (SQLCHAR *)dsn, SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS);
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
