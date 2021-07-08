#include "duckdb_odbc.hpp"

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

/**
 * Get the new database name from the DSN string.
 * TODO this can used to get configuration properties for the database
 */
static void GetDatabaseName(SQLCHAR *dsn, string &new_db_name) {
	string dsn_str((char *)dsn);

	auto pos_db = dsn_str.find("Database");
	if (pos_db != string::npos) {
		auto pos_equal = dsn_str.find('=', pos_db);
		if (pos_equal == string::npos) {
			// an equal '=' char must be present (syntax error)
			return;
		}

		auto pos_end_db = dsn_str.find(';', pos_equal);
		if (pos_end_db == string::npos) {
			// there is no ';', reached the string end
			pos_end_db = dsn_str.size();
		}
		new_db_name = dsn_str.substr(pos_equal + 1, pos_end_db - pos_equal - 1);
	}
}

SQLRETURN SQLDriverConnect(SQLHDBC connection_handle, SQLHWND window_handle, SQLCHAR *in_connection_string,
                           SQLSMALLINT string_length1, SQLCHAR *out_connection_string, SQLSMALLINT buffer_length,
                           SQLSMALLINT *string_length2_ptr, SQLUSMALLINT driver_completion) {
	// TODO actually interpret Database in in_connection_string
	if (!connection_handle) {
		return SQL_ERROR;
	}
	auto *dbc = (duckdb::OdbcHandleDbc *)connection_handle;
	if (dbc->type != duckdb::OdbcHandleType::DBC) {
		return SQL_ERROR;
	}

	string db_name;
	GetDatabaseName(in_connection_string, db_name);
	if (!db_name.empty()) {
		dbc->env->db = duckdb::make_unique<duckdb::DuckDB>(db_name);
	}

	if (!dbc->conn || !db_name.empty()) {
		dbc->conn = duckdb::make_unique<duckdb::Connection>(*dbc->env->db);
		dbc->conn->SetAutoCommit(dbc->autocommit);
	}
	return SQL_SUCCESS;
}

SQLRETURN SQLConnect(SQLHDBC connection_handle, SQLCHAR *server_name, SQLSMALLINT name_length1, SQLCHAR *user_name,
                     SQLSMALLINT name_length2, SQLCHAR *authentication, SQLSMALLINT name_length3) {
	// TODO this is duplicated from above, but server_name is just the DSN
	if (!connection_handle) {
		return SQL_ERROR;
	}
	auto *dbc = (duckdb::OdbcHandleDbc *)connection_handle;
	if (dbc->type != duckdb::OdbcHandleType::DBC) {
		return SQL_ERROR;
	}
	if (!dbc->conn) {
		dbc->conn = duckdb::make_unique<duckdb::Connection>(*dbc->env->db);
		dbc->conn->SetAutoCommit(dbc->autocommit);
	}
	return SQL_SUCCESS;
}

SQLRETURN SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number, SQLCHAR *sql_state,
                        SQLINTEGER *native_error_ptr, SQLCHAR *message_text, SQLSMALLINT buffer_length,
                        SQLSMALLINT *text_length_ptr) {

	if (!handle) {
		return SQL_ERROR;
	}
	if (rec_number != 1) { // TODO is it just trying increasing rec numbers?
		return SQL_ERROR;
	}

	if (sql_state) {
		*sql_state = 0;
	}

	if (native_error_ptr) {
		*native_error_ptr = 0; // we don't have error codes
	}

	auto *hdl = (duckdb::OdbcHandle *)handle;

	switch (handle_type) {
	case SQL_HANDLE_DBC: {
		// TODO return connection errors here
		return SQL_ERROR;
	}
	case SQL_HANDLE_DESC:
		throw std::runtime_error("SQL_HANDLE_DESC");
	case SQL_HANDLE_ENV: {
		// dont think we can have errors here
		return SQL_ERROR;
	}
	case SQL_HANDLE_STMT: {
		if (hdl->type != duckdb::OdbcHandleType::STMT) {
			return SQL_ERROR;
		}
		auto *stmt = (duckdb::OdbcHandleStmt *)hdl;
		if (stmt->stmt && !stmt->stmt->success) {
			duckdb::OdbcUtils::WriteString(stmt->stmt->error, message_text, buffer_length, text_length_ptr);
			return SQL_SUCCESS;
		}
		if (stmt->res && !stmt->res->success) {
			duckdb::OdbcUtils::WriteString(stmt->res->error, message_text, buffer_length, text_length_ptr);
			return SQL_SUCCESS;
		}
		return SQL_ERROR;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number,
                          SQLSMALLINT diag_identifier, SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                          SQLSMALLINT *string_length_ptr) {
	throw std::runtime_error("SQLGetDiagField");
}
