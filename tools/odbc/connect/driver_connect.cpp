#include "connect.hpp"

#include "duckdb_odbc.hpp"
#include "odbc_utils.hpp"

using duckdb::OdbcUtils;

static SQLRETURN ConvertDBCBeforeConnection(SQLHDBC connection_handle, duckdb::OdbcHandleDbc *&dbc) {
	if (!connection_handle) {
		return SQL_INVALID_HANDLE;
	}
	dbc = static_cast<duckdb::OdbcHandleDbc *>(connection_handle);
	if (dbc->type != duckdb::OdbcHandleType::DBC) {
		return SQL_INVALID_HANDLE;
	}
	return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDriverConnect(SQLHDBC connection_handle, SQLHWND window_handle, SQLCHAR *in_connection_string,
                                   SQLSMALLINT string_length1, SQLCHAR *out_connection_string,
                                   SQLSMALLINT buffer_length, SQLSMALLINT *string_length2_ptr,
                                   SQLUSMALLINT driver_completion) {
	duckdb::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertDBCBeforeConnection(connection_handle, dbc);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}

	duckdb::Connect connect(dbc, OdbcUtils::ConvertSQLCHARToString(in_connection_string));

	ret = connect.ParseInputStr();
	if (!connect.SetSuccessWithInfo(ret)) {
		return ret;
	}

	ret = connect.SetConnection();
	if (!connect.SetSuccessWithInfo(ret)) {
		return ret;
	}

	std::string connect_str = "DuckDB connection";
	if (string_length2_ptr) {
		*string_length2_ptr = connect_str.size();
	}
	if (out_connection_string) {
		memcpy(out_connection_string, connect_str.c_str(),
		       duckdb::MinValue<SQLSMALLINT>((SQLSMALLINT)connect_str.size(), buffer_length));
	}
	return connect.GetSuccessWithInfo() ? SQL_SUCCESS_WITH_INFO : ret;
}

SQLRETURN SQL_API SQLConnect(SQLHDBC connection_handle, SQLCHAR *server_name, SQLSMALLINT name_length1,
                             SQLCHAR *user_name, SQLSMALLINT name_length2, SQLCHAR *authentication,
                             SQLSMALLINT name_length3) {
	duckdb::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertDBCBeforeConnection(connection_handle, dbc);
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}

	duckdb::Connect connect(dbc, OdbcUtils::ConvertSQLCHARToString(server_name));

	return connect.SetConnection();
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC connection_handle) {
	duckdb::OdbcHandleDbc *dbc = nullptr;
	SQLRETURN ret = ConvertConnection(connection_handle, dbc);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	dbc->conn.reset();
	return SQL_SUCCESS;
}
