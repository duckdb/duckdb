#ifndef STATEMENT_FUNCTIONS_HPP
#define STATEMENT_FUNCTIONS_HPP

#pragma once

#include "duckdb_odbc.hpp"

namespace duckdb {

SQLRETURN PrepareStmt(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length);

SQLRETURN ExecuteStmt(SQLHSTMT statement_handle);

SQLRETURN FetchStmtResult(SQLHSTMT statement_handle, SQLSMALLINT fetch_orientation = SQL_FETCH_NEXT);

SQLRETURN GetDataStmtResult(SQLHSTMT statement_handle, SQLUSMALLINT col_or_param_num, SQLSMALLINT target_type,
                            SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr);

} // namespace duckdb
#endif
