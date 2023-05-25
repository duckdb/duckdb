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

namespace odbc_test {
void ODBC_CHECK(SQLRETURN ret, SQLSMALLINT tpe, SQLHANDLE hnd, const char *func);
void METADATA_CHECK(SQLRETURN &ret, SQLHSTMT hstmt, SQLUSMALLINT col_num, const char *expected_col_name,
                           SQLSMALLINT expected_col_name_len, SQLSMALLINT expected_col_data_type,
                           SQLULEN expected_col_size, SQLSMALLINT expected_col_decimal_digits,
                           SQLSMALLINT expected_col_nullable);
void CONNECT_TO_DATABASE(SQLRETURN &ret, SQLHANDLE &env, SQLHANDLE &dbc);
void DISCONNECT_FROM_DATABASE(SQLRETURN &ret, SQLHANDLE &dbc, SQLHANDLE &env);
} // namespace odbc_test

#endif // ODBC_TEST_COMMON_H
