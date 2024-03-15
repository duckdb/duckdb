#include "handle_functions.hpp"
#include "odbc_fetch.hpp"
#include "odbc_utils.hpp"
#include "parameter_descriptor.hpp"

#include <codecvt>

SQLRETURN duckdb::SetDiagnosticRecord(OdbcHandle *handle, const SQLRETURN &ret, const std::string &component,
                                      const std::string &msg, const SQLStateType &sqlstate_type,
                                      const std::string &server_name) {
	DiagRecord diag_rec(msg, sqlstate_type, server_name);

	handle->odbc_diagnostic->FormatDiagnosticMessage(diag_rec, server_name, component);
	handle->odbc_diagnostic->AddDiagRecord(diag_rec);
	return ret;
}

SQLRETURN duckdb::ConvertHandle(SQLHANDLE &handle, OdbcHandle *&hdl) {
	if (!handle) {
		return SQL_INVALID_HANDLE;
	}

	hdl = static_cast<OdbcHandle *>(handle);
	if (!hdl->odbc_diagnostic) {
		return SQL_INVALID_HANDLE;
	}

	return SQL_SUCCESS;
}

SQLRETURN duckdb::ConvertEnvironment(SQLHANDLE &environment_handle, OdbcHandleEnv *&env) {
	if (!environment_handle) {
		return SQL_INVALID_HANDLE;
	}
	env = static_cast<OdbcHandleEnv *>(environment_handle);
	if (env->type != OdbcHandleType::ENV) {
		return SQL_INVALID_HANDLE;
	}
	if (!env->db) {
		return SetDiagnosticRecord(env, SQL_ERROR, "env", "No database connection found", SQLStateType::ST_HY000, "");
	}

	return SQL_SUCCESS;
}

SQLRETURN duckdb::ConvertConnection(SQLHANDLE &connection_handle, OdbcHandleDbc *&dbc) {
	if (!connection_handle) {
		return SQL_INVALID_HANDLE;
	}
	dbc = static_cast<OdbcHandleDbc *>(connection_handle);
	if (dbc->type != OdbcHandleType::DBC) {
		return SQL_INVALID_HANDLE;
	}
	if (!dbc->conn) {
		return SetDiagnosticRecord(dbc, SQL_ERROR, "dbc", "No database connection found", SQLStateType::ST_08003, "");
	}

	// ODBC requires to clean up the diagnostic for every ODBC function call
	dbc->odbc_diagnostic->Clean();

	return SQL_SUCCESS;
}

SQLRETURN duckdb::ConvertHSTMT(SQLHANDLE &statement_handle, OdbcHandleStmt *&hstmt) {
	if (!statement_handle) {
		return SQL_INVALID_HANDLE;
	}
	hstmt = static_cast<OdbcHandleStmt *>(statement_handle);
	if (hstmt->type != OdbcHandleType::STMT) {
		return SQL_INVALID_HANDLE;
	}
	if (!hstmt->dbc || !hstmt->dbc->conn) {
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "dbc", "No database connection found", SQLStateType::ST_08003, "");
	}

	// ODBC requires to clean up the diagnostic for every ODBC function call
	hstmt->odbc_diagnostic->Clean();

	return SQL_SUCCESS;
}

SQLRETURN duckdb::ConvertHSTMTPrepared(SQLHANDLE &statement_handle, OdbcHandleStmt *&hstmt) {
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}
	if (!hstmt->stmt) {
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "stmt", "No statement found", SQLStateType::ST_HY000, "");
	}
	if (hstmt->stmt->HasError()) {
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "stmt", hstmt->stmt->GetError(), SQLStateType::ST_HY000,
		                           hstmt->dbc->GetDataSourceName());
	}
	return SQL_SUCCESS;
}

SQLRETURN duckdb::ConvertHSTMTResult(SQLHANDLE &statement_handle, OdbcHandleStmt *&hstmt) {
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}
	if (!hstmt->res) {
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "stmt", "No result set found", SQLStateType::ST_HY000,
		                           hstmt->dbc->GetDataSourceName());
	}
	if (hstmt->res->HasError()) {
		return SetDiagnosticRecord(hstmt, SQL_ERROR, "stmt", hstmt->stmt->GetError(), SQLStateType::ST_HY000,
		                           hstmt->dbc->GetDataSourceName());
	}
	return SQL_SUCCESS;
}

SQLRETURN duckdb::ConvertDescriptor(SQLHANDLE &descriptor_handle, duckdb::OdbcHandleDesc *&desc) {
	if (!descriptor_handle) {
		return SQL_INVALID_HANDLE;
	}

	desc = static_cast<OdbcHandleDesc *>(descriptor_handle);
	if (desc->type != OdbcHandleType::DESC) {
		return SQL_INVALID_HANDLE;
	}
	return SQL_SUCCESS;
}
