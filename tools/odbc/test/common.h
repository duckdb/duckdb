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

using namespace std;

namespace odbc_test {
void ODBC_CHECK(SQLRETURN ret, SQLSMALLINT tpe, SQLHANDLE hnd, const char *func);
void ACCESS_DIAGNOSTIC(string &state, string &message, SQLHANDLE handle, SQLRETURN &ret, SQLSMALLINT handle_type);
void DATA_CHECK(HSTMT hstmt, SQLSMALLINT col_num, const char *expected_content);
void METADATA_CHECK(HSTMT hstmt, SQLUSMALLINT col_num, const char *expected_col_name, SQLSMALLINT expected_col_name_len,
                    SQLSMALLINT expected_col_data_type, SQLULEN expected_col_size,
                    SQLSMALLINT expected_col_decimal_digits, SQLSMALLINT expected_col_nullable);
void DRIVER_CONNECT_TO_DATABASE(SQLRETURN &ret, SQLHANDLE &env, SQLHANDLE &dbc, const string &extra_params);
void CONNECT_TO_DATABASE(SQLRETURN &ret, SQLHANDLE &env, SQLHANDLE &dbc);
void DISCONNECT_FROM_DATABASE(SQLRETURN &ret, SQLHANDLE &dbc, SQLHANDLE &env);
void INITIALIZE_DATABASE(HSTMT hstmt);
} // namespace odbc_test

#endif // ODBC_TEST_COMMON_H
