#ifndef HANDLE_FUNCTIONS_HPP
#define HANDLE_FUNCTIONS_HPP

#pragma once

#include "duckdb_odbc.hpp"

namespace duckdb {

/**
 * @brief Sets the diagnostic record for the given handle and returns the given return value
 * @param handle
 * @param ret
 * @param component Name of the component that caused the error (e.g. "SQLPrepare")
 * @param msg
 * @param sqlstate_type The SQLSTATE type (e.g. ST_22001)
 * @param server_name
 * @return
 */
SQLRETURN SetDiagnosticRecord(OdbcHandle *handle, const SQLRETURN &ret, const std::string &component,
                              const std::string &msg, const SQLStateType &sqlstate_type,
                              const std::string &server_name);

/**
 * @brief Converts the given handle to an OdbcHandle
 * @param handle
 * @param hdl
 * @return
 */
SQLRETURN ConvertHandle(SQLHANDLE &handle, OdbcHandle *&hdl);

/**
 * @brief Converts the given handle to an OdbcHandleEnv
 * @param environment_handle
 * @param env
 * @return
 */
SQLRETURN ConvertEnvironment(SQLHANDLE &environment_handle, OdbcHandleEnv *&env);

/**
 * @brief Converts the given handle to an OdbcHandleDbc
 * @param connection_handle
 * @param dbc
 * @return
 */
SQLRETURN ConvertConnection(SQLHANDLE &connection_handle, OdbcHandleDbc *&dbc);

/**
 * @brief Converts the given handle to an OdbcHandleStmt
 * @param statement_handle
 * @param hstmt
 * @return
 */
SQLRETURN ConvertHSTMT(SQLHANDLE &statement_handle, OdbcHandleStmt *&hstmt);

/**
 * @brief Converts the given handle to an OdbcHandleStmt and checks if it is prepared
 * @param statement_handle
 * @param hstmt
 * @return
 */
SQLRETURN ConvertHSTMTPrepared(SQLHANDLE &statement_handle, OdbcHandleStmt *&hstmt);

/**
 * @brief Converts the given handle to an OdbcHandleStmt and checks if it is executed
 * @param statement_handle
 * @param hstmt
 * @return
 */
SQLRETURN ConvertHSTMTResult(SQLHANDLE &statement_handle, OdbcHandleStmt *&hstmt);

/**
 * @brief Converts the given handle to an OdbcHandleDesc
 * @param descriptor_handle
 * @param desc
 * @return
 */
SQLRETURN ConvertDescriptor(SQLHANDLE &descriptor_handle, OdbcHandleDesc *&desc);

} // namespace duckdb

#endif // HANDLE_FUNCTIONS_HPP
