#include "../common.h"

using namespace odbc_test;

#define MAX_INSERT_COUNT 2
#define MAX_BUFFER_SIZE  100

TEST_CASE("Test binding multiple columsn at once", "[odbc]") {
	SQLHANDLE env;
	SQLHANDLE dbc;

	HSTMT hstmt = SQL_NULL_HSTMT;

	// Connect to the database using SQLConnect
	CONNECT_TO_DATABASE(env, dbc);

	// Allocate a statement handle
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Create a table
	EXECUTE_AND_CHECK("SQLExecDirect (CREATE TABLE)", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("CREATE TABLE test_tbl (Column1 VARCHAR(100), Column2 VARCHAR(100))"), SQL_NTS);

	// Free and re-allocate the statement handle to clear the statement
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);
	EXECUTE_AND_CHECK("SQLAllocHandle (HSTMT)", SQLAllocHandle, SQL_HANDLE_STMT, dbc, &hstmt);

	// Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use column-wise binding.
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_PARAM_BIND_TYPE)", SQLSetStmtAttr, hstmt, SQL_ATTR_PARAM_BIND_TYPE,
	                  reinterpret_cast<SQLPOINTER>(SQL_PARAM_BIND_BY_COLUMN), 0);

	// Specify an array in which to return the status of each set of parameters.
	SQLUSMALLINT param_status[MAX_INSERT_COUNT];
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_PARAM_STATUS_PTR)", SQLSetStmtAttr, hstmt, SQL_ATTR_PARAM_STATUS_PTR,
	                  param_status, 0);

	// Specify an SQLULEN value into which to return the number of sets of parameters processed.
	SQLULEN params_processed;
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_PARAMS_PROCESSED_PTR)", SQLSetStmtAttr, hstmt,
	                  SQL_ATTR_PARAMS_PROCESSED_PTR, &params_processed, 0);

	// Specify the number of parameter sets to be processed before execution occurs.
	EXECUTE_AND_CHECK("SQLSetStmtAttr (SQL_ATTR_PARAMSET_SIZE)", SQLSetStmtAttr, hstmt, SQL_ATTR_PARAMSET_SIZE,
	                  ConvertToSQLPOINTER(MAX_INSERT_COUNT), 0);

	const char *c1_r1 = "John Doe", *c1_r2 = "Jane Doe";
	const char *c2_r1 = "John", *c2_r2 = "Jane";

	SQLLEN c1_ind[MAX_INSERT_COUNT] = {static_cast<SQLLEN>(strlen(c1_r1)), static_cast<SQLLEN>(strlen(c1_r2))};
	SQLLEN c2_ind[MAX_INSERT_COUNT] = {static_cast<SQLLEN>(strlen(c2_r1)), static_cast<SQLLEN>(strlen(c2_r2))};

	char c1[MAX_INSERT_COUNT][MAX_BUFFER_SIZE];
	memcpy(c1[0], c1_r1, c1_ind[0]);
	memcpy(c1[1], c1_r2, c1_ind[1]);

	char c2[MAX_INSERT_COUNT][MAX_BUFFER_SIZE];
	memcpy(c2[0], c2_r1, c2_ind[0]);
	memcpy(c2[1], c2_r2, c2_ind[1]);

	// Bind the parameters in column-wise fashion.
	EXECUTE_AND_CHECK("SQLBindParameter", SQLBindParameter, hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
	                  MAX_BUFFER_SIZE - 1, 0, c1, MAX_BUFFER_SIZE, c1_ind);
	EXECUTE_AND_CHECK("SQLBindParameter", SQLBindParameter, hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
	                  MAX_BUFFER_SIZE - 1, 0, c2, MAX_BUFFER_SIZE, c2_ind);

	// Execute the statement
	EXECUTE_AND_CHECK("SQLExecDirect (INSERT)", SQLExecDirect, hstmt,
	                  ConvertToSQLCHAR("INSERT INTO test_tbl (Column1, Column2) VALUES (?, ?)"), SQL_NTS);

	// Verify that the correct number of parameter sets were processed.
	for (int i = 0; i < params_processed; i++) {
		REQUIRE(param_status[i] == SQL_PARAM_SUCCESS);
	}

	// Close the cursor
	EXECUTE_AND_CHECK("SQLFreeStmt (SQL_CLOSE)", SQLFreeStmt, hstmt, SQL_CLOSE);

	// Get the data back and verify it
	EXECUTE_AND_CHECK("SQLExecDirect (SELECT)", SQLExecDirect, hstmt, ConvertToSQLCHAR("SELECT * FROM test_tbl"),
	                  SQL_NTS);

	std::string col_name[2] = {"Column1", "Column2"};
	std::map<SQLSMALLINT, SQLULEN> types_map = InitializeTypesMap();

	for (int i = 0; i >= 0; i++) {
		SQLRETURN ret = SQLFetch(hstmt);
		if (ret == SQL_NO_DATA) {
			break;
		}
		ODBC_CHECK(ret, "SQLFetch");
		for (int j = 0; j < params_processed; j++) {
			EXECUTE_AND_CHECK("SQLGetData", SQLGetData, hstmt, 1, SQL_C_CHAR, c1[j], MAX_BUFFER_SIZE, &c1_ind[j]);
			EXECUTE_AND_CHECK("SQLGetData", SQLGetData, hstmt, 2, SQL_C_CHAR, c2[j], MAX_BUFFER_SIZE, &c2_ind[j]);
		}
	}

	// Free the statement handle
	EXECUTE_AND_CHECK("SQLFreeStmt (HSTMT)", SQLFreeStmt, hstmt, SQL_CLOSE);
	EXECUTE_AND_CHECK("SQLFreeHandle (HSTMT)", SQLFreeHandle, SQL_HANDLE_STMT, hstmt);

	DISCONNECT_FROM_DATABASE(env, dbc);
}
