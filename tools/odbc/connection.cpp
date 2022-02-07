#include "driver.hpp"
#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "odbc_utils.hpp"

#include "duckdb/common/helper.hpp"

using duckdb::OdbcUtils;
using std::ptrdiff_t;

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                    SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {

	return duckdb::WithConnection(connection_handle, [&](duckdb::OdbcHandleDbc *dbc) {
		if (!value_ptr) {
			return SQL_ERROR;
		}
		switch (attribute) {
		case SQL_ATTR_AUTOCOMMIT:
			*(SQLUINTEGER *)value_ptr = dbc->autocommit;
			return SQL_SUCCESS;
		case SQL_ATTR_ACCESS_MODE:
			*(SQLUINTEGER *)value_ptr = dbc->sql_attr_access_mode;
			return SQL_SUCCESS;
		case SQL_ATTR_CURRENT_CATALOG: {
			if (value_ptr == nullptr) {
				*string_length_ptr = dbc->sql_attr_current_catalog.size();
				dbc->error_messages.emplace_back("SQLGetConnectAttr returned with info.");
				return SQL_SUCCESS_WITH_INFO;
			}

			auto ret = SQL_SUCCESS;
			auto out_len = duckdb::MinValue(dbc->sql_attr_current_catalog.size(), (size_t)buffer_length);
			memcpy((char *)value_ptr, dbc->sql_attr_current_catalog.c_str(), out_len);

			if (out_len == (size_t)buffer_length) {
				ret = SQL_SUCCESS_WITH_INFO;
				out_len = buffer_length - 1;
				dbc->error_messages.emplace_back("SQLGetConnectAttr returned with info.");
			}

			((char *)value_ptr)[out_len] = '\0';

			if (string_length_ptr) {
				*string_length_ptr = out_len;
			}

			return ret;
		}
#ifdef SQL_ATTR_ASYNC_DBC_EVENT
		case SQL_ATTR_ASYNC_DBC_EVENT:
#endif
		case SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE:
#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
		case SQL_ATTR_ASYNC_DBC_PCALLBACK:
#endif
#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
		case SQL_ATTR_ASYNC_DBC_PCONTEXT:
#endif
		case SQL_ATTR_ASYNC_ENABLE:
		case SQL_ATTR_AUTO_IPD:
		case SQL_ATTR_CONNECTION_DEAD:
		case SQL_ATTR_CONNECTION_TIMEOUT:
#ifdef SQL_ATTR_DBC_INFO_TOKEN
		case SQL_ATTR_DBC_INFO_TOKEN:
#endif
		case SQL_ATTR_ENLIST_IN_DTC:
		case SQL_ATTR_LOGIN_TIMEOUT:
		case SQL_ATTR_METADATA_ID:
		case SQL_ATTR_ODBC_CURSORS:
		case SQL_ATTR_PACKET_SIZE:
		case SQL_ATTR_QUIET_MODE:
		case SQL_ATTR_TRACE:
		case SQL_ATTR_TRACEFILE:
		case SQL_ATTR_TRANSLATE_LIB:
		case SQL_ATTR_TRANSLATE_OPTION:
		case SQL_ATTR_TXN_ISOLATION:
			return SQL_NO_DATA;
		default:
			dbc->error_messages.emplace_back("Attribute not supported.");
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                    SQLINTEGER string_length) {
	return duckdb::WithConnection(connection_handle, [&](duckdb::OdbcHandleDbc *dbc) {
		switch (attribute) {
		case SQL_ATTR_AUTOCOMMIT:
			switch ((ptrdiff_t)value_ptr) {
			case (ptrdiff_t)SQL_AUTOCOMMIT_ON:
				dbc->autocommit = true;
				dbc->conn->SetAutoCommit(true);
				return SQL_SUCCESS;
			case (ptrdiff_t)SQL_AUTOCOMMIT_OFF:
				dbc->autocommit = false;
				dbc->conn->SetAutoCommit(false);
				return SQL_SUCCESS;
			case SQL_ATTR_METADATA_ID:
				dbc->sql_attr_metadata_id = *((SQLUINTEGER *)value_ptr);
				return SQL_SUCCESS;
			default:
				return SQL_ERROR;
			}
			break;
		case SQL_ATTR_ACCESS_MODE:
			dbc->sql_attr_access_mode = *((SQLUINTEGER *)value_ptr);
			return SQL_SUCCESS;
#ifdef SQL_ATTR_ASYNC_DBC_EVENT
		case SQL_ATTR_ASYNC_DBC_EVENT:
#endif
		case SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE:
#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
		case SQL_ATTR_ASYNC_DBC_PCALLBACK:
#endif
#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
		case SQL_ATTR_ASYNC_DBC_PCONTEXT:
#endif
		case SQL_ATTR_ASYNC_ENABLE:
			dbc->error_messages.emplace_back("DuckDB does not support asynchronous events.");
			return SQL_ERROR;
		case SQL_ATTR_AUTO_IPD:
		case SQL_ATTR_CONNECTION_DEAD:
			dbc->error_messages.emplace_back("Read-only attribute.");
			return SQL_ERROR;
		case SQL_ATTR_CONNECTION_TIMEOUT:
			dbc->error_messages.emplace_back("DuckDB does not support connection timeout.");
			return SQL_ERROR;
		case SQL_ATTR_CURRENT_CATALOG:
			if (string_length == SQL_NTS) {
				dbc->sql_attr_current_catalog = std::string((char *)value_ptr);
			} else {
				dbc->sql_attr_current_catalog = std::string((char *)value_ptr, string_length);
			}
			return SQL_SUCCESS;
#ifdef SQL_ATTR_DBC_INFO_TOKEN
		case SQL_ATTR_DBC_INFO_TOKEN:
#endif
		case SQL_ATTR_ENLIST_IN_DTC:
		case SQL_ATTR_LOGIN_TIMEOUT:
		case SQL_ATTR_METADATA_ID:
		case SQL_ATTR_ODBC_CURSORS:
		case SQL_ATTR_PACKET_SIZE:
		case SQL_ATTR_QUIET_MODE:
		case SQL_ATTR_TRACE:
		case SQL_ATTR_TRACEFILE:
		case SQL_ATTR_TRANSLATE_LIB:
		case SQL_ATTR_TRANSLATE_OPTION:
		case SQL_ATTR_TXN_ISOLATION:
			dbc->error_messages.emplace_back("Optional feature not supported.");
			return SQL_ERROR;
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQL_API SQLGetInfo(SQLHDBC connection_handle, SQLUSMALLINT info_type, SQLPOINTER info_value_ptr,
                             SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr) {

	return duckdb::WithConnection(connection_handle, [&](duckdb::OdbcHandleDbc *dbc) -> SQLRETURN {
		// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver15
		if (duckdb::ApiInfo::IsNumericInfoType(info_type) && info_value_ptr == nullptr) {
			dbc->error_messages.emplace_back("Invalid null value pointer for numeric info type");
			return SQL_ERROR;
		}

		// Default strings: YES or NO
		std::string yes_str("Y");
		std::string no_str("N");

		switch (info_type) {
		case SQL_ACCESSIBLE_PROCEDURES: {
			duckdb::OdbcUtils::WriteString(no_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_ACCESSIBLE_TABLES: {
			duckdb::OdbcUtils::WriteString(yes_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_ACTIVE_ENVIRONMENTS: {
			*(SQLUSMALLINT *)info_value_ptr = 0;
			return SQL_SUCCESS;
		}
		case SQL_AGGREGATE_FUNCTIONS: {
			SQLUINTEGER mask =
			    SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT | SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM;
			*(SQLUINTEGER *)info_value_ptr = mask;
			return SQL_SUCCESS;
		}
		case SQL_ALTER_DOMAIN: {
			*(SQLUINTEGER *)info_value_ptr = 0;
			return SQL_SUCCESS;
		}

		case SQL_DRIVER_NAME:
		case SQL_DBMS_NAME: {
			std::string dbname = "DuckDB";
			duckdb::OdbcUtils::WriteString(dbname, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DRIVER_ODBC_VER: {
			std::string driver_ver = "03.00";
			duckdb::OdbcUtils::WriteString(driver_ver, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DBMS_VER: {
			SQLHDBC stmt;

			if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, connection_handle, &stmt))) {
				duckdb::FreeHandle(SQL_HANDLE_STMT, stmt);
				return SQL_ERROR;
			}
			if (!SQL_SUCCEEDED(
			        SQLExecDirect(stmt, (SQLCHAR *)"SELECT library_version FROM pragma_version()", SQL_NTS))) {
				duckdb::FreeHandle(SQL_HANDLE_STMT, stmt);
				return SQL_ERROR;
			}
			if (!SQL_SUCCEEDED(SQLFetch(stmt))) {
				duckdb::FreeHandle(SQL_HANDLE_STMT, stmt);
				return SQL_ERROR;
			}

			SQLRETURN ret;
			if (string_length_ptr) {
				SQLLEN len_ptr;
				ret = SQLGetData(stmt, 1, SQL_C_CHAR, info_value_ptr, buffer_length, &len_ptr);
				*string_length_ptr = len_ptr;
			} else {
				ret = SQLGetData(stmt, 1, SQL_C_CHAR, info_value_ptr, buffer_length, nullptr);
			}
			if (!SQL_SUCCEEDED(ret)) {
				duckdb::FreeHandle(SQL_HANDLE_STMT, stmt);
				return SQL_ERROR;
			}

			duckdb::FreeHandle(SQL_HANDLE_STMT, stmt);
			return SQL_SUCCESS;
		}
		case SQL_NON_NULLABLE_COLUMNS:
			// TODO assert buffer length >= sizeof(SQLUSMALLINT)
			duckdb::Store<SQLUSMALLINT>(SQL_NNC_NON_NULL, (duckdb::data_ptr_t)info_value_ptr);
			return SQL_SUCCESS;
		case SQL_ODBC_INTERFACE_CONFORMANCE:
			// TODO assert buffer length >= sizeof(SQLUINTEGER)

			duckdb::Store<SQLUINTEGER>(SQL_OIC_CORE, (duckdb::data_ptr_t)info_value_ptr);

			return SQL_SUCCESS;

		case SQL_CREATE_TABLE:
			// TODO assert buffer length >= sizeof(SQLUINTEGER)

			duckdb::Store<SQLUINTEGER>(SQL_CT_CREATE_TABLE, (duckdb::data_ptr_t)info_value_ptr);
			return SQL_SUCCESS;

		case SQL_CURSOR_COMMIT_BEHAVIOR:
			duckdb::Store<SQLUSMALLINT>(SQL_CB_PRESERVE, (duckdb::data_ptr_t)info_value_ptr);
			return SQL_SUCCESS;
		case SQL_CURSOR_ROLLBACK_BEHAVIOR:
			duckdb::Store<SQLUSMALLINT>(SQL_CB_CLOSE, (duckdb::data_ptr_t)info_value_ptr);
			return SQL_SUCCESS;
		case SQL_GETDATA_EXTENSIONS:
			SQLUINTEGER mask;
			mask = (SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER | SQL_GD_BOUND | SQL_GD_BLOCK);
			duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
			return SQL_SUCCESS;
		case SQL_IDENTIFIER_QUOTE_CHAR:
			duckdb::OdbcUtils::WriteString("\"", (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		case SQL_TABLE_TERM: {
			auto *dbc = (duckdb::OdbcHandleDbc *)connection_handle;
			const std::string str_table("table");
			return OdbcUtils::SetStringAndLength(dbc->error_messages, str_table, info_value_ptr, buffer_length,
			                                     string_length_ptr);
		}
		default:
			return SQL_ERROR;
		}
	}); // end lambda function
}

SQLRETURN SQL_API SQLEndTran(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT completion_type) {
	if (handle_type != SQL_HANDLE_DBC) { // theoretically this can also be done on env but no no no
		return SQL_ERROR;
	}
	return duckdb::WithConnection(handle, [&](duckdb::OdbcHandleDbc *dbc) {
		switch (completion_type) {
		case SQL_COMMIT:
			// it needs to materialize the result set because ODBC can still fetch after a commit
			if (dbc->MaterializeResult() != SQL_SUCCESS) {
				// for some reason we couldn't materialize the result set
				return SQL_ERROR;
			}
			if (dbc->conn->IsAutoCommit()) {
				return SQL_SUCCESS;
			}
			dbc->conn->Commit();
			return SQL_SUCCESS;
		case SQL_ROLLBACK:
			dbc->conn->Rollback();
			return SQL_SUCCESS;
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC connection_handle) {
	return duckdb::WithConnection(connection_handle, [&](duckdb::OdbcHandleDbc *dbc) {
		dbc->conn.reset();
		return SQL_SUCCESS;
	});
}
