#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "odbc_fetch.hpp"
#include "statement_functions.hpp"
#include "parameter_wrapper.hpp"

using duckdb::LogicalTypeId;

SQLRETURN SQLSetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                         SQLINTEGER string_length) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		switch (attribute) {
		case SQL_ATTR_PARAMSET_SIZE: {
			/* auto size = Load<SQLLEN>((data_ptr_t) value_ptr);
			 return (size == 1) ? SQL_SUCCESS : SQL_ERROR;
			 */
			// this should be 1?
			stmt->param_wrapper->paramset_size = (SQLULEN)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_PARAMS_PROCESSED_PTR:
			stmt->param_wrapper->SetParamProcessedPtr(value_ptr);
			return SQL_SUCCESS;
		case SQL_ATTR_PARAM_STATUS_PTR:
			stmt->param_wrapper->param_status_ptr = (SQLUSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		case SQL_ATTR_QUERY_TIMEOUT: {
			// this should be 0
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROW_ARRAY_SIZE: {
			// TODO allow fetch to put more rows in bound cols
			if (value_ptr) {
				SQLULEN new_size = (SQLULEN)value_ptr;
				if (new_size < 1) {
					return SQL_ERROR;
				}
				stmt->odbc_fetcher->rowset_size = new_size;
			}
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROWS_FETCHED_PTR: {
			stmt->rows_fetched_ptr = (SQLULEN *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROW_BIND_TYPE: {
			if (value_ptr && (SQLULEN)value_ptr != SQL_BIND_BY_COLUMN) {
				//! it's a row-wise binding orientation (SQLFetch should support it)
				stmt->odbc_fetcher->row_length = (SQLULEN *)value_ptr;
				stmt->odbc_fetcher->bind_orientation = duckdb::FetchBindingOrientation::ROW;
			}
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROW_STATUS_PTR: {
			stmt->odbc_fetcher->row_status_buff = (SQLUSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_CURSOR_TYPE: {
			stmt->odbc_fetcher->cursor_type = (SQLULEN)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_CONCURRENCY:
			// needs to be implemented
			return SQL_SUCCESS;
		default:
			stmt->error_messages.emplace_back("Unsupported attribute type.");
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQLPrepare(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	return duckdb::PrepareStmt(statement_handle, statement_text, text_length);
}

SQLRETURN SQLCancel(SQLHSTMT statement_handle) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		stmt->dbc->conn->Interrupt();
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLExecDirect(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	return duckdb::ExecDirectStmt(statement_handle, statement_text, text_length);
}

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqltables-function
SQLRETURN SQLTables(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1, SQLCHAR *schema_name,
                    SQLSMALLINT name_length2, SQLCHAR *table_name, SQLSMALLINT name_length3, SQLCHAR *table_type,
                    SQLSMALLINT name_length4) {

	auto catalog_n = duckdb::OdbcUtils::ReadString(catalog_name, name_length1);
	auto schema_n = duckdb::OdbcUtils::ReadString(schema_name, name_length2);
	auto table_n = duckdb::OdbcUtils::ReadString(table_name, name_length3);
	auto table_tp = duckdb::OdbcUtils::ReadString(table_type, name_length4);

	// special cases
	if (catalog_n == std::string(SQL_ALL_CATALOGS) && name_length2 == 0 && name_length2 == 0) {
		if (!SQL_SUCCEEDED(duckdb::ExecDirectStmt(statement_handle,
		                                          (SQLCHAR *)"SELECT '' \"TABLE_CAT\", NULL \"TABLE_SCHEM\", NULL "
		                                                     "\"TABLE_NAME\", NULL \"TABLE_TYPE\" , NULL \"REMARKS\"",
		                                          SQL_NTS))) {
			return SQL_ERROR;
		}
		return SQL_SUCCESS;
	}

	if (schema_n == std::string(SQL_ALL_SCHEMAS) && name_length1 == 0 && name_length3 == 0) {
		if (!SQL_SUCCEEDED(duckdb::ExecDirectStmt(
		        statement_handle,
		        (SQLCHAR *)"SELECT '' \"TABLE_CAT\", schema_name \"TABLE_SCHEM\", NULL \"TABLE_NAME\", "
		                   "NULL \"TABLE_TYPE\" , NULL \"REMARKS\" FROM information_schema.schemata",
		        SQL_NTS))) {
			return SQL_ERROR;
		}
		return SQL_SUCCESS;
	}

	if (table_tp == std::string(SQL_ALL_TABLE_TYPES) && name_length1 == 0 && name_length2 == 0 && name_length3 == 0) {
		return SQL_ERROR; // TODO
	}

	// TODO make this a nice template? also going to use this for SQLColumns etc.
	if (!SQL_SUCCEEDED(duckdb::PrepareStmt(
	        statement_handle,
	        (SQLCHAR
	             *)"SELECT table_catalog \"TABLE_CAT\", table_schema \"TABLE_SCHEM\", table_name \"TABLE_NAME\", CASE "
	               "WHEN table_type='BASE TABLE' THEN 'TABLE' ELSE table_type END \"TABLE_TYPE\" , '' \"REMARKS\"  "
	               "FROM information_schema.tables WHERE table_schema LIKE ? AND table_name LIKE ? and table_type = ?",
	        SQL_NTS))) {
		return SQL_ERROR;
	}

	SQLLEN null_indicator = SQL_NULL_DATA;
	if (!SQL_SUCCEEDED(duckdb::BindParameterStmt(statement_handle, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0,
	                                             0, schema_name, name_length2,
	                                             schema_name ? nullptr : &null_indicator))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(duckdb::BindParameterStmt(statement_handle, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0,
	                                             0, table_name, name_length3,
	                                             table_name ? nullptr : &null_indicator))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(duckdb::BindParameterStmt(statement_handle, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0,
	                                             0, table_type, name_length4,
	                                             table_type ? nullptr : &null_indicator))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(duckdb::ExecuteStmt(statement_handle))) {
		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}

SQLRETURN SQLColumns(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1, SQLCHAR *schema_name,
                     SQLSMALLINT name_length2, SQLCHAR *table_name, SQLSMALLINT name_length3, SQLCHAR *column_name,
                     SQLSMALLINT name_length4) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		// TODO
		return SQL_ERROR;
	});
}

SQLRETURN SQLColAttribute(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
                          SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr,
                          SQLLEN *numeric_attribute_ptr) {

	return duckdb::WithStatementPrepared(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (column_number < 1 || column_number > stmt->stmt->GetTypes().size()) {
			stmt->error_messages.emplace_back("Column number out of range.");
			return SQL_ERROR;
		}

		duckdb::idx_t col_idx = column_number - 1;

		switch (field_identifier) {
		case SQL_DESC_LABEL: {
			if (buffer_length <= 0) {
				stmt->error_messages.emplace_back("Inadequate buffer length.");
				return SQL_ERROR;
			}

			auto col_name = stmt->stmt->GetNames()[col_idx];
			auto out_len = duckdb::MinValue(col_name.size(), (size_t)buffer_length);
			memcpy(character_attribute_ptr, col_name.c_str(), out_len);
			((char *)character_attribute_ptr)[out_len] = '\0';

			if (string_length_ptr) {
				*string_length_ptr = out_len;
			}

			return SQL_SUCCESS;
		}
		case SQL_DESC_OCTET_LENGTH:
			// 0 DuckDB doesn't provide octet length
			if (numeric_attribute_ptr) {
				*numeric_attribute_ptr = 0;
			}
			return SQL_SUCCESS;
		case SQL_DESC_TYPE_NAME: {
			if (buffer_length <= 0) {
				stmt->error_messages.emplace_back("Inadequate buffer length.");
				return SQL_ERROR;
			}

			auto internal_type = stmt->stmt->GetTypes()[col_idx].InternalType();
			std::string type_name = duckdb::TypeIdToString(internal_type);
			auto out_len = duckdb::MinValue(type_name.size(), (size_t)buffer_length);
			memcpy(character_attribute_ptr, type_name.c_str(), out_len);
			((char *)character_attribute_ptr)[out_len] = '\0';

			if (string_length_ptr) {
				*string_length_ptr = out_len;
			}

			return SQL_SUCCESS;
		}
		case SQL_DESC_DISPLAY_SIZE: {
			auto ret =
			    duckdb::ApiInfo::GetColumnSize(stmt->stmt->GetTypes()[col_idx], (SQLULEN *)numeric_attribute_ptr);
			if (ret == SQL_ERROR) {
				stmt->error_messages.emplace_back("Unsupported type for display size.");
				return SQL_ERROR;
			}
		}
		case SQL_DESC_UNSIGNED: {
			auto type = stmt->stmt->GetTypes()[col_idx];
			switch (type.id()) {
			case LogicalTypeId::UTINYINT:
			case LogicalTypeId::USMALLINT:
			case LogicalTypeId::UINTEGER:
			case LogicalTypeId::UBIGINT:
				*numeric_attribute_ptr = SQL_TRUE;
				break;
			default:
				*numeric_attribute_ptr = SQL_FALSE;
			}
			return SQL_SUCCESS;
		}
		default:
			stmt->error_messages.emplace_back("Unsupported attribute type.");
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQLFreeStmt(SQLHSTMT statement_handle, SQLUSMALLINT option) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN {
		if (option == SQL_DROP) {
			// mapping FreeStmt with DROP option to SQLFreeHandle
			return SQLFreeHandle(SQL_HANDLE_STMT, statement_handle);
		}
		if (option == SQL_UNBIND) {
			stmt->bound_cols.clear();
			return SQL_SUCCESS;
		}
		if (option == SQL_RESET_PARAMS) {
			stmt->param_wrapper->Clear();
			return SQL_SUCCESS;
		}
		if (option == SQL_CLOSE) {
			stmt->Close();
			return SQL_SUCCESS;
		}
		return SQL_ERROR;
	});
}

SQLRETURN SQLMoreResults(SQLHSTMT statement_handle) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN {
		if (!stmt->param_wrapper->HasParamSetToProcess()) {
			return SQL_NO_DATA;
		}
		return duckdb::SingleExecuteStmt(stmt);
	});
}
