#include "driver.hpp"
#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_exception.hpp"
#include "odbc_utils.hpp"

#include "duckdb/common/helper.hpp"

using duckdb::OdbcUtils;
using duckdb::SQLStateType;
using std::ptrdiff_t;

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                    SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {

	return duckdb::WithConnection(connection_handle, [&](duckdb::OdbcHandleDbc *dbc) {
		if (!value_ptr) {
			return SQL_ERROR;
		}
		switch (attribute) {
		case SQL_ATTR_AUTOCOMMIT: {
			duckdb::Store<SQLUINTEGER>(dbc->autocommit, (duckdb::data_ptr_t)value_ptr);
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ACCESS_MODE: {
			duckdb::Store<SQLUINTEGER>(dbc->sql_attr_access_mode, (duckdb::data_ptr_t)value_ptr);
			return SQL_SUCCESS;
		}
		case SQL_ATTR_CURRENT_CATALOG: {
			if (value_ptr == nullptr) {
				*string_length_ptr = dbc->sql_attr_current_catalog.size();
				duckdb::DiagRecord diag_rec("Catalog attribute with null value pointer.",
				                            SQLStateType::INVALID_ATTR_VALUE, dbc->GetDataSourceName());
				throw duckdb::OdbcException("SQLGetConnectAttr", SQL_SUCCESS_WITH_INFO, diag_rec);
			}

			auto ret = SQL_SUCCESS;
			auto out_len = duckdb::MinValue(dbc->sql_attr_current_catalog.size(), (size_t)buffer_length);
			memcpy((char *)value_ptr, dbc->sql_attr_current_catalog.c_str(), out_len);

			if (out_len == (size_t)buffer_length) {
				ret = SQL_SUCCESS_WITH_INFO;
				out_len = buffer_length - 1;
				duckdb::DiagRecord diag_rec("Catalog attribute length mismatch.", SQLStateType::STR_LEN_MISMATCH,
				                            dbc->GetDataSourceName());
				throw duckdb::OdbcException("SQLGetConnectAttr", SQL_SUCCESS_WITH_INFO, diag_rec);
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
			return SQL_NO_DATA;
		case SQL_ATTR_QUERY_TIMEOUT: {
			*(SQLINTEGER *)value_ptr = 0;
			buffer_length = sizeof(SQLINTEGER);
			return SQL_SUCCESS;
		}
		case SQL_ATTR_TXN_ISOLATION: {
			*(SQLUINTEGER *)value_ptr = SQL_TXN_SERIALIZABLE;
			return SQL_SUCCESS;
		}
		default:
			duckdb::DiagRecord diag_rec("Attribute not supported.", SQLStateType::INVALID_ATTR_OPTION_ID,
			                            dbc->GetDataSourceName());
			throw duckdb::OdbcException("SQLGetConnectAttr", SQL_ERROR, diag_rec);
		}
	});
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                    SQLINTEGER string_length) {
	// attributes before connection
	switch (attribute) {
	case SQL_ATTR_LOGIN_TIMEOUT:
	case SQL_ATTR_ODBC_CURSORS:
	case SQL_ATTR_PACKET_SIZE:
		return SQL_SUCCESS;
	default:
		break;
	}
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
				return SQL_SUCCESS;
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
		case SQL_ATTR_ASYNC_ENABLE: {
			duckdb::DiagRecord diag_rec("DuckDB does not support asynchronous events.",
			                            SQLStateType::INVALID_ATTR_VALUE, dbc->GetDataSourceName());
			throw duckdb::OdbcException("SQLSetConnectAttr", SQL_ERROR, diag_rec);
		}
		case SQL_ATTR_AUTO_IPD:
		case SQL_ATTR_CONNECTION_DEAD: {
			duckdb::DiagRecord diag_rec("Read-only attribute.", SQLStateType::INVALID_ATTR_OPTION_ID,
			                            dbc->GetDataSourceName());
			throw duckdb::OdbcException("SQLSetConnectAttr", SQL_ERROR, diag_rec);
		}
		case SQL_ATTR_CONNECTION_TIMEOUT:
			return SQL_SUCCESS;
		case SQL_ATTR_CURRENT_CATALOG: {
			if (dbc->conn) {
				duckdb::DiagRecord diag_rec("Connection already stablished, the database name could not be set.",
				                            SQLStateType::INVALID_CONNECTION_STR_ATTR, dbc->GetDataSourceName());
				throw duckdb::OdbcException("SQLSetConnectAttr", SQL_ERROR, diag_rec);
			}
			if (string_length == SQL_NTS) {
				dbc->sql_attr_current_catalog = std::string((char *)value_ptr);
			} else {
				dbc->sql_attr_current_catalog = std::string((char *)value_ptr, string_length);
			}
			return SQL_SUCCESS;
		}
#ifdef SQL_ATTR_DBC_INFO_TOKEN
		case SQL_ATTR_DBC_INFO_TOKEN:
#endif
		case SQL_ATTR_ENLIST_IN_DTC:
		case SQL_ATTR_METADATA_ID:
		case SQL_ATTR_QUIET_MODE:
		case SQL_ATTR_TRACE:
		case SQL_ATTR_TRACEFILE:
		case SQL_ATTR_TRANSLATE_LIB:
		case SQL_ATTR_TRANSLATE_OPTION:
		case SQL_ATTR_TXN_ISOLATION: {
			return SQL_SUCCESS;
		}
		default:
			duckdb::DiagRecord diag_rec("Option value changed:" + std::to_string(attribute),
			                            SQLStateType::OPTION_VALUE_CHANGED, dbc->GetDataSourceName());
			throw duckdb::OdbcException("SQLSetConnectAttr", SQL_SUCCESS_WITH_INFO, diag_rec);
		}
	});
}

SQLRETURN SQL_API SQLGetInfo(SQLHDBC connection_handle, SQLUSMALLINT info_type, SQLPOINTER info_value_ptr,
                             SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr) {

	// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver15

	// verify numeric info value type and null value pointer
	if (duckdb::ApiInfo::IsNumericInfoType(info_type) && info_value_ptr == nullptr) {
		return duckdb::WithConnection(connection_handle, [&](duckdb::OdbcHandleDbc *dbc) -> SQLRETURN {
			duckdb::DiagRecord diag_rec("Invalid null value pointer for numeric info type.",
			                            SQLStateType::INVALID_ATTR_VALUE, dbc->GetDataSourceName());
			throw duckdb::OdbcException("SQLGetInfo", SQL_ERROR, diag_rec);
		});
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
		duckdb::Store<SQLUSMALLINT>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_AGGREGATE_FUNCTIONS: {
		SQLUINTEGER mask =
		    SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT | SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ALTER_DOMAIN: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ALTER_TABLE: {
		// options suppoerd by the DuckDB's tables
		SQLUINTEGER mask = SQL_AT_ADD_COLUMN_COLLATION | SQL_AT_ADD_COLUMN_DEFAULT | SQL_AT_ADD_COLUMN_SINGLE |
		                   SQL_AT_ADD_CONSTRAINT | SQL_AT_ADD_TABLE_CONSTRAINT | SQL_AT_DROP_COLUMN_DEFAULT |
		                   SQL_AT_SET_COLUMN_DEFAULT;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ASYNC_DBC_FUNCTIONS: {
		duckdb::Store<SQLUINTEGER>(SQL_ASYNC_DBC_NOT_CAPABLE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ASYNC_MODE: {
		duckdb::Store<SQLUINTEGER>(SQL_AM_NONE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
#ifdef SQL_ASYNC_NOTIFICATION
	case SQL_ASYNC_NOTIFICATION: {
		duckdb::Store<SQLUINTEGER>(SQL_ASYNC_NOTIFICATION_NOT_CAPABLE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
#endif
	case SQL_BATCH_ROW_COUNT: {
		duckdb::Store<SQLUINTEGER>(SQL_BRC_EXPLICIT, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_BATCH_SUPPORT: {
		SQLUINTEGER mask = SQL_BS_SELECT_EXPLICIT | SQL_BS_ROW_COUNT_EXPLICIT;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_BOOKMARK_PERSISTENCE: {
		// we do not support bookmark, it's is implicit in DuckDB
		/* SQL_BP_CLOSE |
		 * SQL_BP_DELETE |
		 * SQL_BP_DROP |
		 * SQL_BP_OTHER_HSTMT |
		 * SQL_BP_TRANSACTION |
		 * SQL_BP_UPDATE */
		return SQL_SUCCESS;
	}
	case SQL_CATALOG_LOCATION: {
		duckdb::Store<SQLUSMALLINT>(SQL_CL_START, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CATALOG_NAME: {
		duckdb::OdbcUtils::WriteString(yes_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CATALOG_NAME_SEPARATOR: {
		std::string cat_separator(".");
		duckdb::OdbcUtils::WriteString(cat_separator, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CATALOG_TERM: {
		std::string empty_str("");
		duckdb::OdbcUtils::WriteString(empty_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CATALOG_USAGE: {
		SQLUINTEGER mask = SQL_CU_DML_STATEMENTS | SQL_CU_TABLE_DEFINITION;
		/* | SQL_CU_PRIVILEGE_DEFINITION |
		 * SQL_CU_INDEX_DEFINITION
		 * SQL_CU_PROCEDURE_INVOCATION |
		 */
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_COLLATION_SEQ: {
		std::string default_collation("UTF-8");
		duckdb::OdbcUtils::WriteString(default_collation, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_COLUMN_ALIAS: {
		duckdb::OdbcUtils::WriteString(yes_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONCAT_NULL_BEHAVIOR: {
		duckdb::Store<SQLUSMALLINT>(SQL_CB_NON_NULL, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	// basically, we used the same conversion rules from MonetBD, it's needed to be tested
	case SQL_CONVERT_TINYINT:
	case SQL_CONVERT_SMALLINT:
	case SQL_CONVERT_INTEGER:
	case SQL_CONVERT_BIGINT: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_BIT | SQL_CVT_CHAR | SQL_CVT_DECIMAL | SQL_CVT_DOUBLE |
		                   SQL_CVT_FLOAT | SQL_CVT_INTEGER | SQL_CVT_LONGVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_REAL |
		                   SQL_CVT_SMALLINT | SQL_CVT_TINYINT | SQL_CVT_VARCHAR;

		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_BINARY:
	case SQL_CONVERT_LONGVARBINARY:
	case SQL_CONVERT_VARBINARY: {
		SQLUINTEGER mask = SQL_CVT_BINARY | SQL_CVT_CHAR | SQL_CVT_LONGVARBINARY | SQL_CVT_LONGVARCHAR |
		                   SQL_CVT_VARBINARY | SQL_CVT_VARCHAR;

		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_BIT: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_BIT | SQL_CVT_CHAR | SQL_CVT_INTEGER | SQL_CVT_LONGVARCHAR |
		                   SQL_CVT_SMALLINT | SQL_CVT_TINYINT | SQL_CVT_VARCHAR;

		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_CHAR:
	case SQL_CONVERT_VARCHAR:
	case SQL_CONVERT_LONGVARCHAR: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_BINARY | SQL_CVT_BIT | SQL_CVT_CHAR | SQL_CVT_DATE |
		                   SQL_CVT_DECIMAL | SQL_CVT_DOUBLE | SQL_CVT_FLOAT | SQL_CVT_INTEGER |
		                   SQL_CVT_INTERVAL_DAY_TIME | SQL_CVT_INTERVAL_YEAR_MONTH | SQL_CVT_LONGVARBINARY |
		                   SQL_CVT_LONGVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_REAL | SQL_CVT_SMALLINT | SQL_CVT_TIME |
		                   SQL_CVT_TIMESTAMP | SQL_CVT_TINYINT | SQL_CVT_VARBINARY | SQL_CVT_VARCHAR;

		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_GUID: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_DATE: {
		SQLUINTEGER mask = SQL_CVT_CHAR | SQL_CVT_DATE | SQL_CVT_LONGVARCHAR | SQL_CVT_TIMESTAMP | SQL_CVT_VARCHAR;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_DECIMAL:
	case SQL_CONVERT_NUMERIC: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_CHAR | SQL_CVT_DECIMAL | SQL_CVT_DOUBLE | SQL_CVT_FLOAT |
		                   SQL_CVT_INTEGER | SQL_CVT_INTERVAL_DAY_TIME | SQL_CVT_LONGVARCHAR | SQL_CVT_NUMERIC |
		                   SQL_CVT_REAL | SQL_CVT_SMALLINT | SQL_CVT_TINYINT | SQL_CVT_VARCHAR;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_DOUBLE:
	case SQL_CONVERT_REAL:
	case SQL_CONVERT_FLOAT: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_CHAR | SQL_CVT_DECIMAL | SQL_CVT_DOUBLE | SQL_CVT_FLOAT |
		                   SQL_CVT_INTEGER | SQL_CVT_LONGVARCHAR | SQL_CVT_NUMERIC | SQL_CVT_REAL | SQL_CVT_SMALLINT |
		                   SQL_CVT_TINYINT | SQL_CVT_VARCHAR;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_INTERVAL_DAY_TIME: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_CHAR | SQL_CVT_INTEGER | SQL_CVT_INTERVAL_DAY_TIME |
		                   SQL_CVT_LONGVARCHAR | SQL_CVT_SMALLINT | SQL_CVT_TIME | SQL_CVT_TINYINT | SQL_CVT_VARCHAR;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_INTERVAL_YEAR_MONTH: {
		SQLUINTEGER mask = SQL_CVT_BIGINT | SQL_CVT_CHAR | SQL_CVT_INTEGER | SQL_CVT_INTERVAL_YEAR_MONTH |
		                   SQL_CVT_LONGVARCHAR | SQL_CVT_SMALLINT | SQL_CVT_TINYINT | SQL_CVT_VARCHAR;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_TIME: {
		SQLUINTEGER mask =
		    SQL_CVT_CHAR | SQL_CVT_INTERVAL_DAY_TIME | SQL_CVT_LONGVARCHAR | SQL_CVT_TIME | SQL_CVT_VARCHAR;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_TIMESTAMP: {
		SQLUINTEGER mask =
		    SQL_CVT_CHAR | SQL_CVT_DATE | SQL_CVT_LONGVARCHAR | SQL_CVT_TIME | SQL_CVT_TIMESTAMP | SQL_CVT_VARCHAR;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
		// end conversion rules

	case SQL_CONVERT_FUNCTIONS: {
		duckdb::Store<SQLUINTEGER>(SQL_FN_CVT_CAST, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CORRELATION_NAME: {
		duckdb::Store<SQLUSMALLINT>(SQL_CN_ANY, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CONVERT_WCHAR:
	case SQL_CONVERT_WLONGVARCHAR:
	case SQL_CONVERT_WVARCHAR:
	case SQL_CREATE_ASSERTION:
	case SQL_CREATE_CHARACTER_SET:
	case SQL_CREATE_COLLATION:
	case SQL_CREATE_DOMAIN:
	case SQL_CREATE_TRANSLATION: {
		// "0" means that the statement is not supported.
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CREATE_SCHEMA: {
		duckdb::Store<SQLUINTEGER>(SQL_CS_CREATE_SCHEMA, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CREATE_TABLE: {
		SQLUINTEGER mask = SQL_CT_COLUMN_CONSTRAINT | SQL_CT_COLUMN_DEFAULT | SQL_CT_CONSTRAINT_NAME_DEFINITION |
		                   SQL_CT_CREATE_TABLE | SQL_CT_LOCAL_TEMPORARY | SQL_CT_TABLE_CONSTRAINT |
		                   SQL_CT_COLUMN_COLLATION;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CREATE_VIEW: {
		SQLUINTEGER mask = SQL_CV_CREATE_VIEW | SQL_CV_CHECK_OPTION;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CURSOR_COMMIT_BEHAVIOR: {
		duckdb::Store<SQLUSMALLINT>(SQL_CB_PRESERVE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CURSOR_ROLLBACK_BEHAVIOR: {
		duckdb::Store<SQLUSMALLINT>(SQL_CB_CLOSE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_CURSOR_SENSITIVITY: {
		duckdb::Store<SQLUINTEGER>(SQL_INSENSITIVE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DATA_SOURCE_NAME: {
		return duckdb::WithConnection(connection_handle, [&](duckdb::OdbcHandleDbc *dbc) -> SQLRETURN {
			duckdb::OdbcUtils::WriteString(dbc->GetDataSourceName(), (SQLCHAR *)info_value_ptr, buffer_length,
			                               string_length_ptr);
			return SQL_SUCCESS;
		});
	}
	case SQL_DATA_SOURCE_READ_ONLY: {
		duckdb::OdbcUtils::WriteString(no_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DATABASE_NAME: {
		return duckdb::WithConnection(connection_handle, [&](duckdb::OdbcHandleDbc *dbc) -> SQLRETURN {
			std::string db_name = dbc->GetDatabaseName();
			if (db_name == ":memory:") {
				db_name = "";
			}
			duckdb::OdbcUtils::WriteString(db_name, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		});
	}
	case SQL_DATETIME_LITERALS: {
		SQLUINTEGER mask = SQL_DL_SQL92_DATE | SQL_DL_SQL92_TIME | SQL_DL_SQL92_TIMESTAMP | SQL_DL_SQL92_INTERVAL_YEAR |
		                   SQL_DL_SQL92_INTERVAL_MONTH | SQL_DL_SQL92_INTERVAL_DAY | SQL_DL_SQL92_INTERVAL_HOUR |
		                   SQL_DL_SQL92_INTERVAL_MINUTE | SQL_DL_SQL92_INTERVAL_SECOND |
		                   SQL_DL_SQL92_INTERVAL_YEAR_TO_MONTH | SQL_DL_SQL92_INTERVAL_DAY_TO_HOUR |
		                   SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTE | SQL_DL_SQL92_INTERVAL_DAY_TO_SECOND |
		                   SQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTE | SQL_DL_SQL92_INTERVAL_HOUR_TO_SECOND |
		                   SQL_DL_SQL92_INTERVAL_MINUTE_TO_SECOND;

		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DRIVER_NAME:
	case SQL_DBMS_NAME: {
		std::string dbname = "DuckDB";
		duckdb::OdbcUtils::WriteString(dbname, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DBMS_VER: {
		SQLHDBC stmt;

		if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, connection_handle, &stmt))) {
			duckdb::FreeHandle(SQL_HANDLE_STMT, stmt);
			return SQL_ERROR;
		}
		if (!SQL_SUCCEEDED(SQLExecDirect(stmt, (SQLCHAR *)"SELECT library_version FROM pragma_version()", SQL_NTS))) {
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
	case SQL_DDL_INDEX: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DEFAULT_TXN_ISOLATION: {
		duckdb::Store<SQLUINTEGER>(SQL_TXN_SERIALIZABLE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DESCRIBE_PARAMETER: {
		duckdb::OdbcUtils::WriteString(yes_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DM_VER: {
		std::string odbc_major = std::to_string(SQL_SPEC_MAJOR);
		std::string odbc_minor = std::to_string(SQL_SPEC_MINOR);
		// this doesn't seem to be so relevant
		std::string dm_build_version = ".####.####";

		std::string dm_version(odbc_major + "." + odbc_minor + dm_build_version);
		duckdb::OdbcUtils::WriteString(dm_version, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
#ifdef SQL_DRIVER_AWARE_POOLING_SUPPORTED
	case SQL_DRIVER_AWARE_POOLING_SUPPORTED: {
		duckdb::Store<SQLUINTEGER>(SQL_DRIVER_AWARE_POOLING_NOT_CAPABLE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
#endif
		// weird info types ("This information type is implemented by the Driver Manager alone.")
		// case SQL_DRIVER_HDBCSQL_DRIVER_HENV:
		// case SQL_DRIVER_HDESC:
		// case SQL_DRIVER_HSTMT:

	case SQL_DRIVER_ODBC_VER: {
		std::string driver_ver = "03.00";
		duckdb::OdbcUtils::WriteString(driver_ver, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DRIVER_VER: {
		std::string driver_ver = "03.00.0000";
		duckdb::OdbcUtils::WriteString(driver_ver, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_ASSERTION: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_CHARACTER_SET: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_COLLATION: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_DOMAIN: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_SCHEMA: {
		SQLUINTEGER mask = SQL_DS_DROP_SCHEMA | SQL_DS_CASCADE | SQL_DS_RESTRICT;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_TABLE: {
		SQLUINTEGER mask = SQL_DT_DROP_TABLE | SQL_DT_CASCADE | SQL_DT_RESTRICT;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_TRANSLATION: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DROP_VIEW: {
		SQLUINTEGER mask = SQL_DV_DROP_VIEW | SQL_DV_CASCADE | SQL_DV_RESTRICT;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES1: {
		SQLUINTEGER mask = SQL_CA1_ABSOLUTE | SQL_CA1_NEXT | SQL_CA1_RELATIVE;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES2: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_EXPRESSIONS_IN_ORDERBY: {
		duckdb::OdbcUtils::WriteString(yes_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_FILE_USAGE: {
		duckdb::Store<SQLUSMALLINT>(SQL_FILE_NOT_SUPPORTED, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1: {
		duckdb::Store<SQLUINTEGER>(SQL_CA1_NEXT, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_GETDATA_EXTENSIONS: {
		SQLUINTEGER mask = SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER | SQL_GD_BOUND | SQL_GD_BLOCK;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_GROUP_BY: {
		duckdb::Store<SQLUSMALLINT>(SQL_GB_NO_RELATION, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_IDENTIFIER_CASE: {
		duckdb::Store<SQLUSMALLINT>(SQL_IC_LOWER, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_IDENTIFIER_QUOTE_CHAR: {
		std::string quote_char("\"");
		duckdb::OdbcUtils::WriteString(quote_char, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_INDEX_KEYWORDS: {
		duckdb::Store<SQLUINTEGER>(SQL_IK_NONE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_INFO_SCHEMA_VIEWS: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_INSERT_STATEMENT: {
		duckdb::Store<SQLUINTEGER>(SQL_IS_INSERT_LITERALS, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_INTEGRITY: {
		duckdb::OdbcUtils::WriteString(no_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_KEYSET_CURSOR_ATTRIBUTES1: {
		SQLUINTEGER mask = SQL_CA1_ABSOLUTE | SQL_CA1_NEXT | SQL_CA1_RELATIVE;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_KEYSET_CURSOR_ATTRIBUTES2: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_KEYWORDS: {
		SQLHSTMT hstmt;

		if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, connection_handle, &hstmt))) {
			duckdb::FreeHandle(SQL_HANDLE_STMT, hstmt);
			return SQL_ERROR;
		}
		if (!SQL_SUCCEEDED(SQLExecDirect(
		        hstmt, (SQLCHAR *)"SELECT keyword_name FROM duckdb_keywords() WHERE keyword_category='reserved'",
		        SQL_NTS))) {
			duckdb::FreeHandle(SQL_HANDLE_STMT, hstmt);
			return SQL_ERROR;
		}

		SQLRETURN rc;
		const size_t keyword_size = 100;
		SQLCHAR *keyword = (SQLCHAR *)malloc(sizeof(SQLCHAR) * keyword_size);
		std::string reserved_keywords;
		while ((rc = SQLFetch(hstmt)) != SQL_NO_DATA) {
			if (!SQL_SUCCEEDED(SQLGetData(hstmt, 1, SQL_C_CHAR, keyword, keyword_size, nullptr))) {
				duckdb::FreeHandle(SQL_HANDLE_STMT, hstmt);
				free(keyword);
				return SQL_ERROR;
			}
			reserved_keywords += std::string((char *)keyword) + ",";
		}
		if (reserved_keywords.empty()) {
			// remove last inserted comma
			reserved_keywords.pop_back();
		}
		free(keyword);
		duckdb::FreeHandle(SQL_HANDLE_STMT, hstmt);

		duckdb::OdbcUtils::WriteString(reserved_keywords, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_LIKE_ESCAPE_CLAUSE: {
		duckdb::OdbcUtils::WriteString(yes_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
	case SQL_MAX_BINARY_LITERAL_LEN:
	case SQL_MAX_CHAR_LITERAL_LEN:
	case SQL_MAX_INDEX_SIZE:
	case SQL_MAX_ROW_SIZE:
	case SQL_MAX_STATEMENT_LEN: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MAX_CATALOG_NAME_LEN:
	case SQL_MAX_COLUMN_NAME_LEN:
	case SQL_MAX_COLUMNS_IN_GROUP_BY:
	case SQL_MAX_COLUMNS_IN_INDEX:
	case SQL_MAX_COLUMNS_IN_ORDER_BY:
	case SQL_MAX_COLUMNS_IN_SELECT:
	case SQL_MAX_COLUMNS_IN_TABLE:
	case SQL_MAX_CONCURRENT_ACTIVITIES:
	case SQL_MAX_CURSOR_NAME_LEN:
	case SQL_MAX_IDENTIFIER_LEN:
	case SQL_MAX_PROCEDURE_NAME_LEN:
	case SQL_MAX_SCHEMA_NAME_LEN:
	case SQL_MAX_TABLE_NAME_LEN:
	case SQL_MAX_TABLES_IN_SELECT:
	case SQL_MAX_USER_NAME_LEN: {
		duckdb::Store<SQLUSMALLINT>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MAX_DRIVER_CONNECTIONS: {
		// Set in 1, maximum number of active connections
		duckdb::Store<SQLUSMALLINT>(1, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MAX_ROW_SIZE_INCLUDES_LONG: {
		duckdb::OdbcUtils::WriteString(yes_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MULT_RESULT_SETS: {
		// saying NO beucase of SQLite
		duckdb::OdbcUtils::WriteString(no_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_MULTIPLE_ACTIVE_TXN: {
		duckdb::OdbcUtils::WriteString(yes_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_NEED_LONG_DATA_LEN: {
		duckdb::OdbcUtils::WriteString(no_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_NON_NULLABLE_COLUMNS: {
		duckdb::Store<SQLUSMALLINT>(SQL_NNC_NON_NULL, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_NULL_COLLATION: {
		duckdb::Store<SQLUSMALLINT>(SQL_NC_START, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_NUMERIC_FUNCTIONS: {
		SQLUINTEGER mask = SQL_FN_NUM_ABS | SQL_FN_NUM_ACOS | SQL_FN_NUM_ASIN | SQL_FN_NUM_ATAN | SQL_FN_NUM_ATAN2 |
		                   SQL_FN_NUM_CEILING | SQL_FN_NUM_COS | SQL_FN_NUM_COT | SQL_FN_NUM_DEGREES | SQL_FN_NUM_EXP |
		                   SQL_FN_NUM_FLOOR | SQL_FN_NUM_LOG | SQL_FN_NUM_LOG10 | SQL_FN_NUM_MOD | SQL_FN_NUM_PI |
		                   SQL_FN_NUM_POWER | SQL_FN_NUM_RADIANS | SQL_FN_NUM_ROUND | SQL_FN_NUM_SIGN | SQL_FN_NUM_SIN |
		                   SQL_FN_NUM_SQRT | SQL_FN_NUM_TAN;

		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ODBC_INTERFACE_CONFORMANCE: {
		duckdb::Store<SQLUINTEGER>(SQL_OIC_CORE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	//  This is implemented only in the Driver Manager
	// case SQL_ODBC_VER:
	case SQL_OJ_CAPABILITIES: {
		SQLUINTEGER mask = SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL | SQL_OJ_INNER | SQL_OJ_ALL_COMPARISON_OPS;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ORDER_BY_COLUMNS_IN_SELECT: {
		duckdb::OdbcUtils::WriteString(yes_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_PARAM_ARRAY_ROW_COUNTS: {
		duckdb::Store<SQLUINTEGER>(SQL_PARC_BATCH, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_PARAM_ARRAY_SELECTS: {
		duckdb::Store<SQLUINTEGER>(SQL_PAS_BATCH, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_POS_OPERATIONS: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_PROCEDURE_TERM: {
		duckdb::OdbcUtils::WriteString("", (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_PROCEDURES: {
		duckdb::OdbcUtils::WriteString(no_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_QUOTED_IDENTIFIER_CASE: {
		duckdb::Store<SQLUSMALLINT>(SQL_IC_SENSITIVE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ROW_UPDATES: {
		duckdb::OdbcUtils::WriteString(no_str, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SCHEMA_TERM: {
		duckdb::OdbcUtils::WriteString("schema", (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SCHEMA_USAGE: {
		SQLUINTEGER mask = SQL_SU_DML_STATEMENTS | SQL_SU_TABLE_DEFINITION;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SCROLL_OPTIONS: {
		duckdb::Store<SQLUINTEGER>(SQL_SCROLL_OPTIONS, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SEARCH_PATTERN_ESCAPE: {
		duckdb::OdbcUtils::WriteString("\\", (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SERVER_NAME: {
		duckdb::OdbcUtils::WriteString("", (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SPECIAL_CHARACTERS: {
		duckdb::OdbcUtils::WriteString("!%&'()*+,-./;:<=>?@[]^{}|~", (SQLCHAR *)info_value_ptr, buffer_length,
		                               string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL_CONFORMANCE: {
		duckdb::Store<SQLUINTEGER>(SQL_SC_SQL92_ENTRY, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_DATETIME_FUNCTIONS: {
		SQLUINTEGER mask = SQL_SDF_CURRENT_DATE | SQL_SDF_CURRENT_TIME | SQL_SDF_CURRENT_TIMESTAMP;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_FOREIGN_KEY_DELETE_RULE:
	case SQL_SQL92_FOREIGN_KEY_UPDATE_RULE:
	case SQL_SQL92_GRANT: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS: {
		SQLUINTEGER mask = SQL_SNVF_BIT_LENGTH | SQL_SNVF_EXTRACT | SQL_SNVF_OCTET_LENGTH | SQL_SNVF_POSITION;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_PREDICATES: {
		SQLUINTEGER mask = SQL_SP_BETWEEN | SQL_SP_COMPARISON | SQL_SP_EXISTS | SQL_SP_IN | SQL_SP_ISNOTNULL |
		                   SQL_SP_ISNULL | SQL_SP_LIKE;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_RELATIONAL_JOIN_OPERATORS: {
		SQLUINTEGER mask = SQL_SRJO_CROSS_JOIN | SQL_SRJO_FULL_OUTER_JOIN | SQL_SRJO_INNER_JOIN |
		                   SQL_SRJO_LEFT_OUTER_JOIN | SQL_SRJO_NATURAL_JOIN | SQL_SRJO_RIGHT_OUTER_JOIN;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_REVOKE: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_ROW_VALUE_CONSTRUCTOR: {
		SQLUINTEGER mask = SQL_SRVC_VALUE_EXPRESSION | SQL_SRVC_NULL | SQL_SRVC_DEFAULT | SQL_SRVC_ROW_SUBQUERY;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_STRING_FUNCTIONS: {
		SQLUINTEGER mask = SQL_SSF_CONVERT | SQL_SSF_LOWER | SQL_SSF_UPPER | SQL_SSF_SUBSTRING | SQL_SSF_TRIM_BOTH |
		                   SQL_SSF_TRIM_LEADING | SQL_SSF_TRIM_TRAILING;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SQL92_VALUE_EXPRESSIONS: {
		SQLUINTEGER mask = SQL_SVE_CASE | SQL_SVE_CAST | SQL_SVE_COALESCE | SQL_SVE_NULLIF;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_STANDARD_CLI_CONFORMANCE: {
		// do we comply with SQL_SCC_XOPEN_CLI_VERSION1 | SQL_SCC_ISO92_CLI ??
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_STATIC_CURSOR_ATTRIBUTES1: {
		SQLUINTEGER mask = SQL_CA1_NEXT | SQL_CA1_ABSOLUTE | SQL_CA1_RELATIVE;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_STATIC_CURSOR_ATTRIBUTES2: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_STRING_FUNCTIONS: {
		SQLUINTEGER mask = SQL_FN_STR_ASCII | SQL_FN_STR_BIT_LENGTH | SQL_FN_STR_CONCAT | SQL_FN_STR_LCASE |
		                   SQL_FN_STR_LEFT | SQL_FN_STR_LENGTH | SQL_FN_STR_LOCATE | SQL_FN_STR_LTRIM |
		                   SQL_FN_STR_REPEAT | SQL_FN_STR_REPLACE | SQL_FN_STR_RIGHT | SQL_FN_STR_RTRIM |
		                   SQL_FN_STR_SUBSTRING | SQL_FN_STR_UCASE;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SUBQUERIES: {
		SQLUINTEGER mask =
		    SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON | SQL_SQ_EXISTS | SQL_SQ_IN | SQL_SQ_QUANTIFIED;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_SYSTEM_FUNCTIONS: {
		duckdb::Store<SQLUINTEGER>(0, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_TABLE_TERM: {
		duckdb::OdbcUtils::WriteString("table", (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_TIMEDATE_ADD_INTERVALS:
	case SQL_TIMEDATE_DIFF_INTERVALS: {
		SQLUINTEGER mask = SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND | SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR |
		                   SQL_FN_TSI_DAY | SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH | SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_TIMEDATE_FUNCTIONS: {
		SQLUINTEGER mask = SQL_FN_TD_CURRENT_DATE | SQL_FN_TD_CURRENT_TIME | SQL_FN_TD_CURRENT_TIMESTAMP |
		                   SQL_FN_TD_DAYNAME | SQL_FN_TD_DAYOFMONTH | SQL_FN_TD_DAYOFWEEK | SQL_FN_TD_DAYOFYEAR |
		                   SQL_FN_TD_EXTRACT | SQL_FN_TD_HOUR | SQL_FN_TD_MINUTE | SQL_FN_TD_MONTH |
		                   SQL_FN_TD_MONTHNAME | SQL_FN_TD_NOW | SQL_FN_TD_QUARTER | SQL_FN_TD_SECOND | SQL_FN_TD_WEEK |
		                   SQL_FN_TD_YEAR;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_TXN_CAPABLE: {
		duckdb::Store<SQLUSMALLINT>(SQL_TC_ALL, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_TXN_ISOLATION_OPTION: {
		duckdb::Store<SQLUINTEGER>(SQL_TXN_SERIALIZABLE, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_UNION: {
		SQLUINTEGER mask = SQL_U_UNION | SQL_U_UNION_ALL;
		duckdb::Store<SQLUINTEGER>(mask, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_USER_NAME:
	case SQL_XOPEN_CLI_YEAR: {
		duckdb::OdbcUtils::WriteString("", (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	default:
		return duckdb::WithConnection(connection_handle, [&](duckdb::OdbcHandleDbc *dbc) -> SQLRETURN {
			duckdb::DiagRecord diag_rec("Unrecognized attribute.", SQLStateType::INVALID_ATTR_OPTION_ID,
			                            dbc->GetDataSourceName());
			// returning SQL_SUCESS, but with a record message
			throw duckdb::OdbcException("SQLGetInfo", SQL_SUCCESS, diag_rec);
		});
	}
} // end SQLGetInfo

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
			try {
				dbc->conn->Rollback();
				return SQL_SUCCESS;
			} catch (duckdb::Exception &ex) {
				duckdb::DiagRecord diag_rec(std::string(ex.what()), SQLStateType::SQLENDTRAN_ASYNC_FUNCT_EXECUTION,
				                            dbc->GetDataSourceName());
				throw duckdb::OdbcException("SQLEndTran", SQL_ERROR, diag_rec);
			}
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
