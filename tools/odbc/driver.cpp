#include "duckdb_odbc.hpp"
#include "driver.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_fetch.hpp"
#include "odbc_utils.hpp"

#include "duckdb/main/config.hpp"

#include <odbcinst.h>
#include <locale>

using duckdb::OdbcDiagnostic;
using duckdb::OdbcUtils;
using std::string;

SQLRETURN duckdb::FreeHandle(SQLSMALLINT handle_type, SQLHANDLE handle) {
	if (!handle) {
		return SQL_ERROR;
	}

	switch (handle_type) {
	case SQL_HANDLE_DBC: {
		auto *hdl = (duckdb::OdbcHandleDbc *)handle;
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC: {
		auto *hdl = (duckdb::OdbcHandleDesc *)handle;
		hdl->dbc->ResetStmtDescriptors(hdl);
		delete hdl;
		return SQL_ERROR;
	}
	case SQL_HANDLE_ENV: {
		auto *hdl = (duckdb::OdbcHandleEnv *)handle;
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_STMT: {
		auto *hdl = (duckdb::OdbcHandleStmt *)handle;
		hdl->dbc->EraseStmtRef(hdl);
		delete hdl;
		return SQL_SUCCESS;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT handle_type, SQLHANDLE handle) {
	return duckdb::FreeHandle(handle_type, handle);
}

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT handle_type, SQLHANDLE input_handle, SQLHANDLE *output_handle_ptr) {
	switch (handle_type) {
	case SQL_HANDLE_DBC: {
		D_ASSERT(input_handle);
		auto *env = (duckdb::OdbcHandleEnv *)input_handle;
		D_ASSERT(env->type == duckdb::OdbcHandleType::ENV);
		*output_handle_ptr = new duckdb::OdbcHandleDbc(env);
		return SQL_SUCCESS;
	}
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
	case SQL_HANDLE_DESC: {
		D_ASSERT(input_handle);
		auto *dbc = (duckdb::OdbcHandleDbc *)input_handle;
		D_ASSERT(dbc->type == duckdb::OdbcHandleType::DBC);
		*output_handle_ptr = new duckdb::OdbcHandleDesc(dbc);
		return SQL_SUCCESS;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV environment_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                SQLINTEGER string_length) {
	return duckdb::WithEnvironment(environment_handle, [&](duckdb::OdbcHandleEnv *env) {
		switch (attribute) {
		case SQL_ATTR_ODBC_VERSION: {
			switch ((SQLUINTEGER)(intptr_t)value_ptr) {
			case SQL_OV_ODBC3:
				// TODO actually do something with this?
				// auto version = (SQLINTEGER)(uintptr_t)value_ptr;
				return SQL_SUCCESS;
			default:
				env->error_messages.emplace_back("ODBC version not supported.");
				return SQL_ERROR;
			}
		}
		case SQL_ATTR_CONNECTION_POOLING:
			switch ((SQLINTEGER)(intptr_t)value_ptr) {
			case SQL_CP_OFF:
			case SQL_CP_ONE_PER_DRIVER:
			case SQL_CP_ONE_PER_HENV:
				return SQL_SUCCESS;
			default:
				env->error_messages.emplace_back("Connection pool option not supported.");
				return SQL_ERROR;
			}
		case SQL_ATTR_CP_MATCH:
			env->error_messages.emplace_back("Optional feature not supported.");
			return SQL_ERROR;
		case SQL_ATTR_OUTPUT_NTS: /* SQLINTEGER */
			switch (*(SQLINTEGER *)value_ptr) {
			case SQL_TRUE:
				return SQL_SUCCESS;
			default:
				env->error_messages.emplace_back("Optional feature not supported.");
				return SQL_ERROR;
			}
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV environment_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	if (value_ptr == nullptr) {
		return SQL_ERROR;
	}
	auto *env = (duckdb::OdbcHandleEnv *)environment_handle;
	if (env->type != duckdb::OdbcHandleType::ENV) {
		return SQL_ERROR;
	}

	switch (attribute) {
	case SQL_ATTR_ODBC_VERSION:
		*(SQLUINTEGER *)value_ptr = SQL_OV_ODBC3;
		break;
	case SQL_ATTR_CONNECTION_POOLING:
		*(SQLINTEGER *)value_ptr = SQL_CP_OFF;
		break;
	case SQL_ATTR_OUTPUT_NTS:
		*(SQLINTEGER *)value_ptr = SQL_TRUE;
		break;
	case SQL_ATTR_CP_MATCH:
		env->error_messages.emplace_back("Optional feature not supported.");
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
}

/**
 * Get the new database name from the DSN string.
 * Otherwise, try to read the database name from odbc.ini
 */
static void GetDatabaseNameFromDSN(duckdb::OdbcHandleDbc *dbc, SQLCHAR *dsn, string &new_db_name) {
	OdbcUtils::SetValueFromConnStr(dsn, "Database", new_db_name);

	// given preference for the connection attribute
	if (!dbc->sql_attr_current_catalog.empty() && new_db_name.empty()) {
		new_db_name = dbc->sql_attr_current_catalog;
		return;
	}
#ifdef ODBC_LINK_ODBCINST
	if (new_db_name.empty()) {
		string dsn_name;
		OdbcUtils::SetValueFromConnStr(dsn, "DSN", dsn_name);
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

	// set DSN
	OdbcUtils::SetValueFromConnStr(conn_str, "DSN", dbc->dsn);

	string db_name;
	GetDatabaseNameFromDSN(dbc, conn_str, db_name);
	dbc->SetDatabaseName(db_name);
	db_name = dbc->GetDatabaseName();

	if (!db_name.empty()) {
		duckdb::DBConfig config;
		if (dbc->sql_attr_access_mode == SQL_MODE_READ_ONLY) {
			config.access_mode = duckdb::AccessMode::READ_ONLY;
		}
		dbc->env->db = duckdb::make_unique<duckdb::DuckDB>(db_name, &config);
	}

	if (!dbc->conn) {
		dbc->conn = duckdb::make_unique<duckdb::Connection>(*dbc->env->db);
		dbc->conn->SetAutoCommit(dbc->autocommit);
	}
	return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDriverConnect(SQLHDBC connection_handle, SQLHWND window_handle, SQLCHAR *in_connection_string,
                                   SQLSMALLINT string_length1, SQLCHAR *out_connection_string,
                                   SQLSMALLINT buffer_length, SQLSMALLINT *string_length2_ptr,
                                   SQLUSMALLINT driver_completion) {
	auto ret = SetConnection(connection_handle, in_connection_string);
	std::string connect_str = "DuckDB connection";
	if (string_length2_ptr) {
		*string_length2_ptr = connect_str.size();
	}
	if (ret == SQL_SUCCESS && out_connection_string) {
		memcpy(out_connection_string, connect_str.c_str(),
		       duckdb::MinValue<SQLSMALLINT>((SQLSMALLINT)connect_str.size(), buffer_length));
	}
	return ret;
}

SQLRETURN SQL_API SQLConnect(SQLHDBC connection_handle, SQLCHAR *server_name, SQLSMALLINT name_length1,
                             SQLCHAR *user_name, SQLSMALLINT name_length2, SQLCHAR *authentication,
                             SQLSMALLINT name_length3) {
	return SetConnection(connection_handle, server_name);
}

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number, SQLCHAR *sql_state,
                                SQLINTEGER *native_error_ptr, SQLCHAR *message_text, SQLSMALLINT buffer_length,
                                SQLSMALLINT *text_length_ptr) {
	if (!handle) {
		std::string msg_str("Handle is NULL.");
		OdbcUtils::WriteString(msg_str, message_text, buffer_length, text_length_ptr);
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

	// lambda function that writes the diagnostic messages
	std::function<SQLRETURN(duckdb::OdbcHandle *, duckdb::OdbcHandleType)> func_write_diag =
	    [&](duckdb::OdbcHandle *hdl, duckdb::OdbcHandleType target_type) {
		    if (hdl->type != target_type) {
			    std::string msg_str("Handle type " + duckdb::OdbcHandleTypeToString(hdl->type) + " mismatch with " +
			                        duckdb::OdbcHandleTypeToString(target_type));
			    OdbcUtils::WriteString(msg_str, message_text, buffer_length, text_length_ptr);
			    return SQL_SUCCESS;
		    }

		    // Errors should be placed at the error_messages
		    if ((size_t)rec_number <= hdl->error_messages.size()) {
			    OdbcUtils::WriteString(hdl->error_messages[rec_number - 1], message_text, buffer_length,
			                           text_length_ptr);
			    return SQL_SUCCESS;
		    } else {
			    return SQL_NO_DATA;
		    }
	    };

	switch (handle_type) {
	case SQL_HANDLE_ENV: {
		return func_write_diag(hdl, duckdb::OdbcHandleType::ENV);
	}
	case SQL_HANDLE_DBC: {
		return func_write_diag(hdl, duckdb::OdbcHandleType::DBC);
	}
	case SQL_HANDLE_STMT: {
		return func_write_diag(hdl, duckdb::OdbcHandleType::STMT);
	}
	case SQL_HANDLE_DESC: {
		return func_write_diag(hdl, duckdb::OdbcHandleType::DESC);
	}
	default:
		return SQL_INVALID_HANDLE;
	}
}

SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number,
                                  SQLSMALLINT diag_identifier, SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                                  SQLSMALLINT *string_length_ptr) {
	switch (handle_type) {
	case SQL_HANDLE_ENV:
	case SQL_HANDLE_DBC:
	case SQL_HANDLE_STMT:
	case SQL_HANDLE_DESC: {
		return duckdb::WithHandle(handle, [&](duckdb::OdbcHandle *hdl) {
			// diag header fields
			switch (diag_identifier) {
			case SQL_DIAG_CURSOR_ROW_COUNT: {
				// this field is available only for statement handles
				if (hdl->type != duckdb::OdbcHandleType::STMT) {
					return SQL_ERROR;
				}
				duckdb::Store<SQLLEN>(hdl->odbc_diagnostic->header.sql_diag_cursor_row_count,
				                      (duckdb::data_ptr_t)diag_info_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_DYNAMIC_FUNCTION: {
				// this field is available only for statement handles
				if (hdl->type != duckdb::OdbcHandleType::STMT) {
					return SQL_ERROR;
				}
				duckdb::OdbcUtils::WriteString(hdl->odbc_diagnostic->GetDiagDynamicFunction(), (SQLCHAR *)diag_info_ptr,
				                               buffer_length, string_length_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_DYNAMIC_FUNCTION_CODE: {
				// this field is available only for statement handles
				if (hdl->type != duckdb::OdbcHandleType::STMT) {
					return SQL_ERROR;
				}
				duckdb::Store<SQLINTEGER>(hdl->odbc_diagnostic->header.sql_diag_dynamic_function_code,
				                          (duckdb::data_ptr_t)diag_info_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_NUMBER: {
				duckdb::Store<SQLINTEGER>(hdl->odbc_diagnostic->header.sql_diag_number,
				                          (duckdb::data_ptr_t)diag_info_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_RETURNCODE: {
				duckdb::Store<SQLRETURN>(hdl->odbc_diagnostic->header.sql_diag_return_code,
				                         (duckdb::data_ptr_t)diag_info_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_ROW_COUNT: {
				// this field is available only for statement handles
				if (hdl->type != duckdb::OdbcHandleType::STMT) {
					return SQL_ERROR;
				}
				duckdb::Store<SQLLEN>(hdl->odbc_diagnostic->header.sql_diag_return_code,
				                      (duckdb::data_ptr_t)diag_info_ptr);
				return SQL_SUCCESS;
			}
			default:
				break;
			}

			// verify identifier and record index
			if (!OdbcDiagnostic::IsDiagRecordField(diag_identifier)) {
				return SQL_ERROR;
			}
			if (rec_number <= 0) {
				return SQL_ERROR;
			}
			auto rec_idx = rec_number - 1;
			if (!hdl->odbc_diagnostic->VerifyRecordIndex(rec_idx)) {
				return SQL_ERROR;
			}

			const auto diag_record = hdl->odbc_diagnostic->GetDiagRecord(rec_idx);

			// diag record fields
			switch (diag_identifier) {
			case SQL_DIAG_CLASS_ORIGIN: {
				duckdb::OdbcUtils::WriteString(hdl->odbc_diagnostic->GetDiagClassOrigin(rec_idx),
				                               (SQLCHAR *)diag_info_ptr, buffer_length, string_length_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_COLUMN_NUMBER: {
				// this field is available only for statement handles
				if (hdl->type != duckdb::OdbcHandleType::STMT) {
					return SQL_ERROR;
				}
				duckdb::Store<SQLINTEGER>(diag_record.sql_diag_column_number, (duckdb::data_ptr_t)diag_info_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_CONNECTION_NAME: {
				// we do not support connection names
				duckdb::OdbcUtils::WriteString("", (SQLCHAR *)diag_info_ptr, buffer_length, string_length_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_MESSAGE_TEXT: {
				duckdb::OdbcUtils::WriteString(diag_record.sql_diag_message_text, (SQLCHAR *)diag_info_ptr,
				                               buffer_length, string_length_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_NATIVE: {
				duckdb::Store<SQLINTEGER>(diag_record.sql_diag_native, (duckdb::data_ptr_t)diag_info_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_ROW_NUMBER: {
				// this field is available only for statement handles
				if (hdl->type != duckdb::OdbcHandleType::STMT) {
					return SQL_ERROR;
				}
				duckdb::Store<SQLLEN>(diag_record.sql_diag_row_number, (duckdb::data_ptr_t)diag_info_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_SERVER_NAME: {
				duckdb::OdbcUtils::WriteString(diag_record.sql_diag_server_name, (SQLCHAR *)diag_info_ptr,
				                               buffer_length, string_length_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_SQLSTATE: {
				duckdb::OdbcUtils::WriteString(diag_record.sql_diag_sqlstate, (SQLCHAR *)diag_info_ptr, buffer_length,
				                               string_length_ptr);
				return SQL_SUCCESS;
			}
			case SQL_DIAG_SUBCLASS_ORIGIN: {
				duckdb::OdbcUtils::WriteString(hdl->odbc_diagnostic->GetDiagSubclassOrigin(rec_idx),
				                               (SQLCHAR *)diag_info_ptr, buffer_length, string_length_ptr);
				return SQL_SUCCESS;
			}
			default:
				return SQL_ERROR;
			}
		});
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQL_API SQLDataSources(SQLHENV environment_handle, SQLUSMALLINT direction, SQLCHAR *server_name,
                                 SQLSMALLINT buffer_length1, SQLSMALLINT *name_length1_ptr, SQLCHAR *description,
                                 SQLSMALLINT buffer_length2, SQLSMALLINT *name_length2_ptr) {
	auto *env = (duckdb::OdbcHandleEnv *)environment_handle;
	env->error_messages.emplace_back("Driver Manager only function");
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLDrivers(SQLHENV environment_handle, SQLUSMALLINT direction, SQLCHAR *driver_description,
                             SQLSMALLINT buffer_length1, SQLSMALLINT *description_length_ptr,
                             SQLCHAR *driver_attributes, SQLSMALLINT buffer_length2,
                             SQLSMALLINT *attributes_length_ptr) {
	auto *env = (duckdb::OdbcHandleEnv *)environment_handle;
	env->error_messages.emplace_back("Driver Manager only function");
	return SQL_ERROR;
}