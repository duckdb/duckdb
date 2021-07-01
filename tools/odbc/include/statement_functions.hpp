#pragma once

#include "duckdb_odbc.hpp"

namespace duckdb {

SQLRETURN PrepareStmt(SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength);

SQLRETURN ExecuteStmt(SQLHSTMT StatementHandle);

} // namespace duckdb