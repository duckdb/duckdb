#include "../common.h"

using namespace odbc_test;

static void DataAtExecution(HSTMT &hstmt) {
	// Prepare a statement
	EXECUTE_AND_CHECK("SQLPrepare", SQLPrepare, hstmt,
	                  ConvertToSQLCHAR("SELECT id FROM bytea_table WHERE t = ? OR t = ?"), SQL_NTS);

	SQLCHAR *param_1 = ConvertToSQLCHAR("bar");
	SQLLEN param_1_bytes = strlen(ConvertToCString(param_1));
	SQLLEN param_1_len = SQL_DATA_AT_EXEC;
	EXECUTE_AND_CHECK("SQLBindParameter", SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARCHAR,
	                  param_1_bytes, 0, ConvertToSQLPOINTER(1), 0, &param_1_len);

	SQLLEN param_2_len = SQL_DATA_AT_EXEC;
	EXECUTE_AND_CHECK("SQLBindParameter", SQLBindParameter, hstmt, 2, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARCHAR, 6, 0,
	                  ConvertToSQLPOINTER(2), 0, &param_2_len);

	// Execute the statement
	SQLRETURN ret = SQLExecute(hstmt);
	REQUIRE(ret == SQL_NEED_DATA);

	// Set the parameter data
	SQLPOINTER param_id = nullptr;
	while ((ret = SQLParamData(hstmt, &param_id)) == SQL_NEED_DATA) {
		if (param_id == ConvertToSQLPOINTER(1)) {
			EXECUTE_AND_CHECK("SQLPutData", SQLPutData, hstmt, param_1, param_1_bytes);
		} else if (param_id == ConvertToSQLPOINTER(2)) {
			EXECUTE_AND_CHECK("SQLPutData", SQLPutData, hstmt, ConvertToSQLPOINTER("foo"), 3);
			EXECUTE_AND_CHECK("SQLPutData", SQLPutData, hstmt, ConvertToSQLPOINTER("bar"), 3);
		} else {
			FAIL("Unexpected parameter id");
		}
	}
	ODBC_CHECK(ret, "SQLParamData");

	// Fetch the results
	for (int i = 2; i < 4; i++) {
		EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
		DATA_CHECK(hstmt, 0, std::to_string(i));
	}

	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
}

static void ArrayBindingDataAtExecution(HSTMT &hstmt) {
	SQLLEN str_ind[2] = {SQL_DATA_AT_EXEC, SQL_DATA_AT_EXEC};
	SQLUSMALLINT status[2];
	SQLULEN num_processed;

	// Prepare a statement
	EXECUTE_AND_CHECK("SQLPrepare", SQLPrepare, hstmt, ConvertToSQLCHAR("SELECT id FROM bytea_table WHERE t = ?"),
	                  SQL_NTS);

	// Set STMT attributes PARAM_BIND_TYPE, PARAM_STATUS_PTR, PARAMS_PROCESSED_PTR, and PARAMSET_SIZE
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_PARAM_BIND_TYPE)", SQLSetStmtAttr, hstmt, SQL_ATTR_PARAM_BIND_TYPE,
	                  reinterpret_cast<SQLPOINTER>(SQL_PARAM_BIND_BY_COLUMN), 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr(SQL_ATTR_PARAM_STATUS_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_PARAM_STATUS_PTR,
	                  status, 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr(SQL_ATTR_PARAMS_PROCESSED_PTR)", SQLSetStmtAttr, hstmt,
	                  SQL_ATTR_PARAMS_PROCESSED_PTR, &num_processed, 0);
	EXECUTE_AND_CHECK("SQLSetStmtAttr(SQL_ATTR_PARAMSET_SIZE)", SQLSetStmtAttr, hstmt, SQL_ATTR_PARAMSET_SIZE,
	                  ConvertToSQLPOINTER(2), 0);

	// Bind the array
	EXECUTE_AND_CHECK("SQLBindParameter", SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY, 5,
	                  0, ConvertToSQLPOINTER(1), 0, str_ind);

	// Execute the statement
	SQLRETURN ret = SQLExecute(hstmt);
	REQUIRE(ret == SQL_NEED_DATA);

	// Set the parameter data
	SQLPOINTER param_id = nullptr;
	while ((ret = SQLParamData(hstmt, &param_id)) == SQL_NEED_DATA) {
		if (num_processed == 1) {
			EXECUTE_AND_CHECK("SQLPutData", SQLPutData, hstmt, ConvertToSQLPOINTER("foo"), 3);
		} else if (num_processed == 2) {
			EXECUTE_AND_CHECK("SQLPutData", SQLPutData, hstmt, ConvertToSQLPOINTER("barf"), 4);
		} else {
			FAIL("Unexpected parameter id");
		}
	}
	ODBC_CHECK(ret, "SQLParamData");

	for (int i = 0; i < num_processed; i++) {
		REQUIRE(status[i] == SQL_PARAM_SUCCESS);
	}

	// Fetch the results
	for (int i = 4; i; i++) {
		EXECUTE_AND_CHECK("SQLFetch", SQLFetch, hstmt);
		DATA_CHECK(hstmt, 0, std::to_string(i));

		ret = SQLMoreResults(hstmt);
		if (ret == SQL_NO_DATA) {
			break;
		} else if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
			ODBC_CHECK(ret, "SQLMoreResults");
		}
	}
	REQUIRE(SQLFetch(hstmt) == SQL_NO_DATA);
}

TEST_CASE("Test SQLBindParameter, SQLParamData, and SQLPutData", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	InitializeDatabase(hstmt);

	// Tests data-at-execution for a single parameter
	DataAtExecution(hstmt);

	// Tests data-at-execution for an array of parameters
	ArrayBindingDataAtExecution(hstmt);

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
