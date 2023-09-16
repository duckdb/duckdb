#ifndef STATEMENT_FUNCTIONS_HPP
#define STATEMENT_FUNCTIONS_HPP

#pragma once

#include "duckdb_odbc.hpp"

namespace duckdb {

SQLRETURN PrepareStmt(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length);

SQLRETURN BatchExecuteStmt(SQLHSTMT statement_handle);
SQLRETURN SingleExecuteStmt(OdbcHandleStmt *hstmt);

SQLRETURN FetchStmtResult(duckdb::OdbcHandleStmt *hstmt, SQLSMALLINT fetch_orientation = SQL_FETCH_NEXT,
                          SQLLEN fetch_offset = 0);

SQLRETURN GetDataStmtResult(OdbcHandleStmt *hstmt, SQLUSMALLINT col_or_param_num, SQLSMALLINT target_type,
                            SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr);

SQLRETURN ExecDirectStmt(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length);

SQLRETURN ExecuteStmt(SQLHSTMT statement_handle);

SQLRETURN BindParameterStmt(SQLHSTMT statement_handle, SQLUSMALLINT parameter_number, SQLSMALLINT input_output_type,
                            SQLSMALLINT value_type, SQLSMALLINT parameter_type, SQLULEN column_size,
                            SQLSMALLINT decimal_digits, SQLPOINTER parameter_value_ptr, SQLLEN buffer_length,
                            SQLLEN *str_len_or_ind_ptr);

SQLRETURN CloseStmt(duckdb::OdbcHandleStmt *hstmt);

} // namespace duckdb
#endif
