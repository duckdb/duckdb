#include "duckdb_odbc.hpp"
#include <odbcinst.h>

using std::string;

SQLRETURN SQLAllocHandle(SQLSMALLINT handle_type, SQLHANDLE input_handle, SQLHANDLE *output_handle_ptr) {
	switch (handle_type) {
	case SQL_HANDLE_DBC: {
		D_ASSERT(input_handle);
		auto *env = (duckdb::OdbcHandleEnv *)input_handle;
		D_ASSERT(env->type == duckdb::OdbcHandleType::ENV);
		*output_handle_ptr = new duckdb::OdbcHandleDbc(env);
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC:
		throw std::runtime_error("SQL_HANDLE_DESC");
	case SQL_HANDLE_ENV:
		*output_handle_ptr = new duckdb::OdbcHandleEnv();
		return SQL_SUCCESS;
	case SQL_HANDLE_STMT: {
		D_ASSERT(input_handle);
		auto *dbc = (duckdb::OdbcHandleDbc *)input_handle;
		D_ASSERT(dbc->type == duckdb::OdbcHandleType::DBC);
		*output_handle_ptr = new duckdb::OdbcHandleStmt(dbc);
		return SQL_SUCCESS;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLFreeHandle(SQLSMALLINT handle_type, SQLHANDLE handle) {
	if (!handle) {
		return SQL_ERROR;
	}

	switch (handle_type) {
	case SQL_HANDLE_DBC: {
		auto *hdl = (duckdb::OdbcHandleDbc *)handle;
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC:
		throw std::runtime_error("SQL_HANDLE_DESC");
	case SQL_HANDLE_ENV: {
		auto *hdl = (duckdb::OdbcHandleEnv *)handle;
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_STMT: {
		auto *hdl = (duckdb::OdbcHandleStmt *)handle;
		if (hdl == hdl->dbc->stmt_handle) {
			hdl->dbc->stmt_handle = nullptr;
		}
		delete hdl;
		return SQL_SUCCESS;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLSetEnvAttr(SQLHENV environment_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                        SQLINTEGER string_length) {
	if (!environment_handle) {
		return SQL_ERROR;
	}
	auto *env = (duckdb::OdbcHandleEnv *)environment_handle;
	if (env->type != duckdb::OdbcHandleType::ENV) {
		return SQL_ERROR;
	}
	switch (attribute) {
	case SQL_ATTR_ODBC_VERSION: {
		auto version = (SQLINTEGER)(uintptr_t)value_ptr;
		// TODO actually do something with this?
		return SQL_SUCCESS;
	}
	default:
		return SQL_ERROR;
	}
}

static void GetValueFromDSN(const string &dsn, const char *key, string &value) {
	auto pos_key = dsn.find(key);
	if (pos_key != string::npos) {
		auto pos_start_value = dsn.find('=', pos_key);
		if (pos_start_value == string::npos) {
			// an equal '=' char must be present (syntax error)
			return;
		}
		++pos_start_value;
		auto pos_end_value = dsn.find(';', pos_start_value);
		if (pos_end_value == string::npos) {
			// there is no ';', reached the string end
			pos_end_value = dsn.size();
		}
		value = dsn.substr(pos_start_value, pos_end_value - pos_start_value);
	}
}

/**
 * Get the new database name from the DSN string.
 * Otherwise, try to read the database name from odbc.ini
 */
static void GetDatabaseName(SQLCHAR *dsn, string &new_db_name) {
	string dsn_str((char *)dsn);
	GetValueFromDSN(dsn_str, "Database", new_db_name);
#ifdef ODBC_LINK_ODBCINST
	if (new_db_name.empty()) {
		string dsn_name;
		GetValueFromDSN(dsn_str, "DSN", dsn_name);
		if (!dsn_name.empty()) {
			const int MAX_DB_NAME = 256;
			char db_name[MAX_DB_NAME];
			SQLGetPrivateProfileString(dsn_name.c_str(), "Database", "", db_name, MAX_DB_NAME, "odbc.ini");
			new_db_name = string(db_name);
		}
	}
#endif
}

static SQLRETURN SetConnection(SQLHDBC connection_handle, SQLCHAR *conn_str) {
	// TODO actually interpret Database in in_connection_string
	if (!connection_handle) {
		return SQL_ERROR;
	}
	auto *dbc = (duckdb::OdbcHandleDbc *)connection_handle;
	if (dbc->type != duckdb::OdbcHandleType::DBC) {
		return SQL_ERROR;
	}

	string db_name;
	GetDatabaseName(conn_str, db_name);
	if (!db_name.empty()) {
		dbc->env->db = duckdb::make_unique<duckdb::DuckDB>(db_name);
	}

	if (!dbc->conn) {
		dbc->conn = duckdb::make_unique<duckdb::Connection>(*dbc->env->db);
		dbc->conn->SetAutoCommit(dbc->autocommit);
	}
	return SQL_SUCCESS;
}

SQLRETURN SQLDriverConnect(SQLHDBC connection_handle, SQLHWND window_handle, SQLCHAR *in_connection_string,
                           SQLSMALLINT string_length1, SQLCHAR *out_connection_string, SQLSMALLINT buffer_length,
                           SQLSMALLINT *string_length2_ptr, SQLUSMALLINT driver_completion) {
	return SetConnection(connection_handle, in_connection_string);
}

SQLRETURN SQLConnect(SQLHDBC connection_handle, SQLCHAR *server_name, SQLSMALLINT name_length1, SQLCHAR *user_name,
                     SQLSMALLINT name_length2, SQLCHAR *authentication, SQLSMALLINT name_length3) {
	return SetConnection(connection_handle, server_name);
}

SQLRETURN SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number, SQLCHAR *sql_state,
                        SQLINTEGER *native_error_ptr, SQLCHAR *message_text, SQLSMALLINT buffer_length,
                        SQLSMALLINT *text_length_ptr) {
	if (!handle) {
		std::string msg_str("Handle is NULL.");
		duckdb::OdbcUtils::WriteString(msg_str, message_text, buffer_length, text_length_ptr);
		return SQL_INVALID_HANDLE;
	}
	if (rec_number <= 0 || buffer_length < 0) {
		return SQL_ERROR;
	}
	if (message_text) {
		*message_text = '\0';
	}
	if (text_length_ptr) {
		*text_length_ptr = 0;
	}
	if (sql_state) {
		*sql_state = '\0';
	}
	if (native_error_ptr) {
		*native_error_ptr = 0; // we don't have error codes
	}

	auto *hdl = (duckdb::OdbcHandle *)handle;

	switch (handle_type) {
	case SQL_HANDLE_DBC: {
		// TODO return connection errors here
		return SQL_NO_DATA;
	}
	case SQL_HANDLE_DESC: {
		// throw std::runtime_error("SQL_HANDLE_DESC");
		return SQL_NO_DATA;
	}
	case SQL_HANDLE_ENV: {
		// dont think we can have errors here
		return SQL_NO_DATA;
	}
	case SQL_HANDLE_STMT: {
		if (hdl->type != duckdb::OdbcHandleType::STMT) {
			std::string msg_str("Handle type is not a OdbcHandleStmt.");
			duckdb::OdbcUtils::WriteString(msg_str, message_text, buffer_length, text_length_ptr);
			return SQL_SUCCESS;
		}

		auto *stmt = (duckdb::OdbcHandleStmt *)hdl;
		// Errors should be placed at the stmt->error_messages
		if ((size_t)rec_number <= stmt->error_messages.size()) {
			duckdb::OdbcUtils::WriteString(stmt->error_messages[rec_number - 1], message_text, buffer_length,
			                               text_length_ptr);
			return SQL_SUCCESS;
		} else {
			return SQL_NO_DATA;
		}
	}
	default:
		return SQL_INVALID_HANDLE;
	}
}

SQLRETURN SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number,
                          SQLSMALLINT diag_identifier, SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                          SQLSMALLINT *string_length_ptr) {
	throw std::runtime_error("SQLGetDiagField");
}
