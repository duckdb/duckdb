#ifndef STATEMENT_FUNCTIONS_HPP
#define STATEMENT_FUNCTIONS_HPP

#pragma once

#include "duckdb_odbc.hpp"

namespace duckdb {

SQLRETURN PrepareStmt(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length);

SQLRETURN ExecuteStmt(SQLHSTMT statement_handle);

} // namespace duckdb
#endif
