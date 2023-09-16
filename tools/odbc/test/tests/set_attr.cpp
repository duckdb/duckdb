#include "../common.h"

using namespace odbc_test;

TEST_CASE("Test SQL_ATTR_ROW_BIND_TYPE attribute in SQLSetStmtAttr", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Set the statement attribute SQL_ATTR_ROW_BIND_TYPE
	uint64_t row_len = 256;
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_ROW_BIND_TYPE)", SQLSetStmtAttr, hstmt, SQL_ATTR_ROW_BIND_TYPE,
	                  ConvertToSQLPOINTER(row_len), SQL_IS_INTEGER);

	// Check the statement attribute SQL_ATTR_ROW_BIND_TYPE
	SQLULEN buf;
	EXECUTE_AND_CHECK("SQLGetStmtAttr (SQL_ATTR_ROW_BIND_TYPE)", SQLGetStmtAttr, hstmt, SQL_ATTR_ROW_BIND_TYPE, &buf,
	                  sizeof(buf), nullptr);
	REQUIRE(row_len == buf);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}

TEST_CASE("Test SQL_ATTR_ACCESS_MODE and SQL_ATTR_METADATA_ID attribute in SQLSetConnectAttr", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Set the Connect attribute SQL_ATTR_ACCESS_MODE to SQL_MODE_READ_ONLY
	EXECUTE_AND_CHECK("SQLSetConnectAttr (SQL_ATTR_ACCESS_MODE)", SQLSetConnectAttr, dbc, SQL_ATTR_ACCESS_MODE,
	                  ConvertToSQLPOINTER(SQL_MODE_READ_ONLY), SQL_IS_INTEGER);

	// Check the Connect attribute SQL_ATTR_ACCESS_MODE
	SQLUINTEGER buf;
	EXECUTE_AND_CHECK("SQLGetConnectAttr (SQL_ATTR_ACCESS_MODE)", SQLGetConnectAttr, dbc, SQL_ATTR_ACCESS_MODE, &buf,
	                  sizeof(buf), nullptr);
	REQUIRE(SQL_MODE_READ_ONLY == buf);

	// Set the Connect attribute SQL_ATTR_ACCESS_MODE to SQL_MODE_READ_WRITE
	EXECUTE_AND_CHECK("SQLSetConnectAttr (SQL_ATTR_ACCESS_MODE)", SQLSetConnectAttr, dbc, SQL_ATTR_ACCESS_MODE,
	                  (SQLPOINTER)SQL_MODE_READ_WRITE, SQL_IS_INTEGER);

	// Check the Connect attribute SQL_ATTR_ACCESS_MODE
	EXECUTE_AND_CHECK("SQLGetConnectAttr (SQL_ATTR_ACCESS_MODE)", SQLGetConnectAttr, dbc, SQL_ATTR_ACCESS_MODE, &buf,
	                  sizeof(buf), nullptr);
	REQUIRE(SQL_MODE_READ_WRITE == buf);

	// Set the Connect attribute SQL_ATTR_METADATA_ID to SQL_TRUE
	EXECUTE_AND_CHECK("SQLSetConnectAttr (SQL_ATTR_METADATA_ID)", SQLSetConnectAttr, dbc, SQL_ATTR_METADATA_ID,
	                  ConvertToSQLPOINTER(SQL_TRUE), SQL_IS_INTEGER);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
