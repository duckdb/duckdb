#ifndef HANDLE_FUNCTIONS_HPP
#define HANDLE_FUNCTIONS_HPP

#pragma once

#include "duckdb_odbc.hpp"

namespace duckdb {

SQLRETURN SetDiagnosticRecord(OdbcHandle *handle, const SQLRETURN &ret, const std::string &component,
                              const std::string &msg, const SQLStateType &sqlstate_type,
                              const std::string &server_name);
SQLRETURN ConvertHandle(SQLHANDLE &handle, OdbcHandle *&hdl);
SQLRETURN ConvertEnvironment(SQLHANDLE &environment_handle, OdbcHandleEnv *&env);
SQLRETURN ConvertConnection(SQLHANDLE &connection_handle, OdbcHandleDbc *&dbc);
SQLRETURN ConvertHSTMT(SQLHANDLE &statement_handle, OdbcHandleStmt *&hstmt);
SQLRETURN ConvertHSTMTPrepared(SQLHANDLE &statement_handle, OdbcHandleStmt *&hstmt);
SQLRETURN ConvertHSTMTResult(SQLHANDLE &statement_handle, OdbcHandleStmt *&hstmt);
SQLRETURN ConvertDescriptor(SQLHANDLE &descriptor_handle, OdbcHandleDesc *&desc);

} // namespace duckdb

#endif // HANDLE_FUNCTIONS_HPP
