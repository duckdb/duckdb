#ifndef ODBC_TEST_COMMON_H
#define ODBC_TEST_COMMON_H

#include "catch.hpp"
#include "odbc_utils.hpp"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>

#define STR_EQUAL(a, b) (std::strcmp(a, b) == 0)

namespace odbc_test {
struct MetadataData {
	std::string col_name;
	SQLSMALLINT col_type;
};

void ODBC_CHECK(SQLRETURN ret, std::string func);

/**
 * @brief
 * Two for one special! Runs the function passed as a param and then checks the return value using ODBC_CHECK.
 * @param msg The message to print if the return value is not SQL_SUCCESS
 * @param func The function to run
 * @param args The arguments to pass to the function
 */
template <typename MSG, typename FUNC, typename... ARGS>
void EXECUTE_AND_CHECK(MSG msg, FUNC func, ARGS... args) {
	SQLRETURN ret = func(args...);
	ODBC_CHECK(ret, msg);
}

/**
 * @brief
 * Runs SQLGetDiagRec to get the diagnostic record of the last function call.
 * The function continues to call SQLGetDiagRec until there are no more records to retrieve.
 *
 * @param state Takes an empty string and returns the SQLSTATE of the last function call
 * @param message Takes an empty string and returns the message of the last function call
 * @param handle A handle for the diagnostic data structure, indicated by HandleType
 * @param handle_type The handle type identifier: SQL_HANDLE_DBC, SQL_HANDLE_DBC_INFO_TOKEN, SQL_HANDLE_DESC,
 * SQL_HANDLE_ENV, SQL_HANDLE_STMT
 */
void ACCESS_DIAGNOSTIC(std::string &state, std::string &message, SQLHANDLE handle, SQLSMALLINT handle_type);

/**
 * @brief
 * Runs SQLGetData to get the data from the column of the current row and compares it with the expected content.
 *
 * @param hstmt A statement handle
 * @param col_num The number of the column in the result set
 * @param expected_content The expected content of the column
 */
void DATA_CHECK(HSTMT &hstmt, SQLSMALLINT col_num, const std::string expected_content);

/**
 * @brief
 * Runs SQLDescribeCol to get the metadata of the column and compares it with the expected metadata.
 * If the expected metadata is not provided (ie empty or NULL), the function won't check for it.
 *
 * @param hstmt A statement handle
 * @param col_num The number of the column in the result set
 * @param expected_col_name
 * @param expected_col_name_len
 * @param expected_col_data_type
 * @param expected_col_size
 * @param expected_col_decimal_digits
 * @param expected_col_nullable
 */
void METADATA_CHECK(HSTMT &hstmt, SQLUSMALLINT col_num, const std::string &expected_col_name,
                    SQLSMALLINT expected_col_name_len, SQLSMALLINT expected_col_data_type, SQLULEN expected_col_size,
                    SQLSMALLINT expected_col_decimal_digits, SQLSMALLINT expected_col_nullable);

/**
 * @brief
 * Connects to the database using the DSN and extra parameters provided and running SQLDriverConnect.
 * It also allocates the environment and connection handles.
 * @param env The environment handle, allocated by the function
 * @param dbc The connection handle, allocated by the function
 * @param extra_params The extra parameters to pass to SQLDriverConnect, can be UserId/Username, Password, Port,
 * driver/database specific options, SSL, ReadOnly, etc
 */
void DRIVER_CONNECT_TO_DATABASE(SQLHANDLE &env, SQLHANDLE &dbc, const std::string &extra_params);

/**
 * @brief
 * Connects to the database using the DSN provided and running SQLConnect.
 * It also allocates the environment and connection handles.
 * @param env The environment handle, allocated by the function
 * @param dbc The connection handle, allocated by the function
 */
void CONNECT_TO_DATABASE(SQLHANDLE &env, SQLHANDLE &dbc);

/**
 * @brief
 * Disconnects from the database and frees the environment and connection handles.
 * @param env The environment handle
 * @param dbc The connection handle
 */
void DISCONNECT_FROM_DATABASE(SQLHANDLE &env, SQLHANDLE &dbc);

/**
 * @brief
 * Runs SQLExecDirect to execute the query provided.
 * @param hstmt A statement handle
 * @param query The query to execute
 */
void EXEC_SQL(HSTMT hstmt, const std::string &query);

void InitializeDatabase(HSTMT &hstmt);

std::map<SQLSMALLINT, SQLULEN> InitializeTypesMap();

// Converters
SQLCHAR *ConvertToSQLCHAR(const char *str);
SQLCHAR *ConvertToSQLCHAR(const std::string &str);
std::string ConvertToString(SQLCHAR *str);
const char *ConvertToCString(SQLCHAR *str);
SQLPOINTER ConvertToSQLPOINTER(uint64_t ptr);
SQLPOINTER ConvertToSQLPOINTER(const char *str);
std::string ConvertHexToString(SQLCHAR val[16], int precision);

} // namespace odbc_test

#endif // ODBC_TEST_COMMON_H
