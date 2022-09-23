#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "driver.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_fetch.hpp"
#include "odbc_utils.hpp"
#include "parameter_descriptor.hpp"
#include "row_descriptor.hpp"
#include "statement_functions.hpp"

#include "duckdb/common/constants.hpp"

#include <regex>

using duckdb::LogicalTypeId;
using duckdb::OdbcUtils;
using duckdb::SQLStateType;
using std::string;

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                 SQLINTEGER string_length) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		switch (attribute) {
		case SQL_ATTR_PARAMSET_SIZE: {
			stmt->param_desc->apd->header.sql_desc_array_size = (SQLULEN)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_PARAM_BIND_TYPE: {
			if (value_ptr) {
				stmt->param_desc->apd->header.sql_desc_bind_type = (SQLINTEGER)(SQLULEN)(uintptr_t)value_ptr;
			}
			return SQL_SUCCESS;
		}
		case SQL_ATTR_PARAMS_PROCESSED_PTR: {
			stmt->param_desc->SetParamProcessedPtr((SQLULEN *)value_ptr);
			return SQL_SUCCESS;
		}
		case SQL_ATTR_PARAM_STATUS_PTR: {
			stmt->param_desc->SetArrayStatusPtr((SQLUSMALLINT *)value_ptr);
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROW_ARRAY_SIZE: {
			// TODO allow fetch to put more rows in bound cols
			if (value_ptr) {
				SQLULEN new_size = (SQLULEN)value_ptr;
				if (new_size < 1) {
					return SQL_ERROR;
				}
				// This field in the ARD can also be set by calling SQLSetStmtAttr with the SQL_ATTR_ROW_ARRAY_SIZE
				// attribute.
				stmt->row_desc->ard->header.sql_desc_array_size = new_size;
			}
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROWS_FETCHED_PTR: {
			stmt->rows_fetched_ptr = (SQLULEN *)value_ptr;
			stmt->row_desc->ird->header.sql_desc_rows_processed_ptr = (SQLULEN *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROW_BIND_TYPE: {
			if (value_ptr == nullptr) {
				return SQL_ERROR;
			}
			stmt->row_desc->ard->header.sql_desc_bind_type = *(SQLULEN *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROW_STATUS_PTR: {
			stmt->row_desc->ird->header.sql_desc_array_status_ptr = (SQLUSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_CURSOR_TYPE: {
			stmt->odbc_fetcher->cursor_type = (SQLULEN)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_APP_ROW_DESC: {
			stmt->SetARD((duckdb::OdbcHandleDesc *)value_ptr);
			return SQL_SUCCESS;
		}
		case SQL_ATTR_APP_PARAM_DESC: {
			stmt->SetAPD((duckdb::OdbcHandleDesc *)value_ptr);
			return SQL_SUCCESS;
		}
		case SQL_ATTR_IMP_PARAM_DESC:
		case SQL_ATTR_IMP_ROW_DESC: {
			duckdb::DiagRecord diag_rec("Option value changed:" + std::to_string(attribute),
			                            SQLStateType::INVALID_USE_AUTO_ALLOC_DESCRIPTOR,
			                            stmt->dbc->GetDataSourceName());
			throw duckdb::OdbcException("SQLSetStmtAttr", SQL_ERROR, diag_rec);
		}
		case SQL_ATTR_PARAM_BIND_OFFSET_PTR: {
			stmt->param_desc->SetBindOffesetPtr((SQLLEN *)value_ptr);
			return SQL_SUCCESS;
		}
		case SQL_ATTR_CONCURRENCY: {
			SQLULEN value = (SQLULEN)(uintptr_t)value_ptr;
			if (value != SQL_CONCUR_LOCK) {
				duckdb::DiagRecord diag_rec("Option value changed:" + std::to_string(attribute),
				                            SQLStateType::OPTION_VALUE_CHANGED, stmt->dbc->GetDataSourceName());
				throw duckdb::OdbcException("SQLSetStmtAttr", SQL_SUCCESS_WITH_INFO, diag_rec);
			}
			return SQL_SUCCESS;
		}
		case SQL_ATTR_QUERY_TIMEOUT:
			return SQL_SUCCESS;
		case SQL_ATTR_RETRIEVE_DATA: {
			SQLULEN value = (SQLULEN)(uintptr_t)value_ptr;
			switch (value) {
			case SQL_RD_ON:
			case SQL_RD_OFF:
				break;
			default:
				/* Invalid attribute value */
				duckdb::DiagRecord diag_rec("Invalid attribute value: " + std::to_string(attribute),
				                            SQLStateType::INVALID_ATTR_VALUE, stmt->dbc->GetDataSourceName());
				throw duckdb::OdbcException("SQLSetStmtAttr", SQL_ERROR, diag_rec);
			}
			stmt->retrieve_data = value;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_CURSOR_SCROLLABLE: {
			SQLULEN value = (SQLULEN)(uintptr_t)value_ptr;
			switch (value) {
			case SQL_NONSCROLLABLE:
				stmt->odbc_fetcher->cursor_type = SQL_CURSOR_FORWARD_ONLY;
				break;
			case SQL_SCROLLABLE:
				stmt->odbc_fetcher->cursor_type = SQL_CURSOR_STATIC;
				break;
			default:
				duckdb::DiagRecord diag_rec("Invalid attribute value:" + std::to_string(attribute),
				                            SQLStateType::INVALID_ATTR_VALUE, stmt->dbc->GetDataSourceName());
				throw duckdb::OdbcException("SQLSetStmtAttr", SQL_ERROR, diag_rec);
			}
			stmt->odbc_fetcher->cursor_scrollable = value;
			return SQL_SUCCESS;
		}
		default:
			duckdb::DiagRecord diag_rec("Option value changed:" + std::to_string(attribute),
			                            SQLStateType::OPTION_VALUE_CHANGED, stmt->dbc->GetDataSourceName());
			throw duckdb::OdbcException("SQLSetStmtAttr", SQL_SUCCESS_WITH_INFO, diag_rec);
		}
	});
}

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                 SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		switch (attribute) {
		case SQL_ATTR_APP_PARAM_DESC:
		case SQL_ATTR_IMP_PARAM_DESC:
		case SQL_ATTR_APP_ROW_DESC:
		case SQL_ATTR_IMP_ROW_DESC: {
			if (string_length_ptr) {
				*string_length_ptr = 4;
			}
			if (attribute == SQL_ATTR_APP_PARAM_DESC) {
				*((HSTMT *)value_ptr) = stmt->param_desc->GetAPD();
			}
			if (attribute == SQL_ATTR_IMP_PARAM_DESC) {
				*((HSTMT *)value_ptr) = stmt->param_desc->GetIPD();
			}
			if (attribute == SQL_ATTR_APP_ROW_DESC) {
				*((HSTMT *)value_ptr) = stmt->row_desc->GetARD();
			}
			if (attribute == SQL_ATTR_IMP_ROW_DESC) {
				*((HSTMT *)value_ptr) = stmt->row_desc->GetIRD();
			}
			return SQL_SUCCESS;
		}
		case SQL_ATTR_PARAM_BIND_OFFSET_PTR: {
			if (value_ptr == nullptr) {
				return SQL_ERROR;
			}
			*((SQLLEN **)value_ptr) = stmt->param_desc->GetBindOffesetPtr();
			return SQL_SUCCESS;
		}
		case SQL_ATTR_PARAM_BIND_TYPE: {
			if (value_ptr == nullptr) {
				return SQL_ERROR;
			}
			*((SQLINTEGER *)value_ptr) = stmt->param_desc->apd->header.sql_desc_bind_type;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_PARAMS_PROCESSED_PTR: {
			if (value_ptr == nullptr) {
				return SQL_ERROR;
			}
			*(SQLULEN **)value_ptr = stmt->param_desc->GetParamProcessedPtr();
			return SQL_SUCCESS;
		}
		case SQL_ATTR_PARAMSET_SIZE: {
			if (value_ptr == nullptr) {
				return SQL_ERROR;
			}
			*(SQLULEN *)value_ptr = stmt->param_desc->apd->header.sql_desc_array_size;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROW_ARRAY_SIZE: {
			if (value_ptr == nullptr) {
				return SQL_ERROR;
			}
			*(SQLULEN *)value_ptr = stmt->row_desc->ard->header.sql_desc_array_size;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROWS_FETCHED_PTR: {
			if (value_ptr == nullptr) {
				return SQL_ERROR;
			}
			*(SQLULEN **)value_ptr = stmt->rows_fetched_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROW_BIND_TYPE: {
			if (value_ptr == nullptr) {
				return SQL_ERROR;
			}
			if ((SQLULEN)value_ptr != SQL_BIND_BY_COLUMN) {
				//! it's a row-wise binding orientation
				*(SQLULEN *)value_ptr = stmt->row_desc->ard->header.sql_desc_bind_type;
				return SQL_SUCCESS;
			}
			*(SQLULEN *)value_ptr = SQL_BIND_BY_COLUMN;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROW_STATUS_PTR: {
			if (value_ptr == nullptr) {
				return SQL_ERROR;
			}
			*(SQLUSMALLINT **)value_ptr = stmt->row_desc->ird->header.sql_desc_array_status_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_CURSOR_TYPE: {
			if (value_ptr == nullptr) {
				return SQL_ERROR;
			}
			*(SQLULEN *)value_ptr = stmt->odbc_fetcher->cursor_type;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_CONCURRENCY: {
			//*(SQLULEN *)value_ptr = SQL_CONCUR_READ_ONLY;
			*(SQLULEN *)value_ptr = SQL_CONCUR_LOCK;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_QUERY_TIMEOUT: {
			*((SQLINTEGER *)value_ptr) = 0;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_RETRIEVE_DATA: {
			*((SQLULEN *)value_ptr) = stmt->retrieve_data;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_CURSOR_SCROLLABLE: {
			*((SQLULEN *)value_ptr) = stmt->odbc_fetcher->cursor_scrollable;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ASYNC_ENABLE:
#ifdef SQL_ATTR_ASYNC_STMT_EVENT
		case SQL_ATTR_ASYNC_STMT_EVENT:
#endif
		case SQL_ATTR_CURSOR_SENSITIVITY:
		case SQL_ATTR_ENABLE_AUTO_IPD:
		case SQL_ATTR_FETCH_BOOKMARK_PTR:
		case SQL_ATTR_KEYSET_SIZE:
		case SQL_ATTR_MAX_LENGTH:
		case SQL_ATTR_MAX_ROWS:
		case SQL_ATTR_METADATA_ID:
		case SQL_ATTR_NOSCAN:
		case SQL_ATTR_PARAM_OPERATION_PTR:
		case SQL_ATTR_PARAM_STATUS_PTR:
		case SQL_ATTR_ROW_BIND_OFFSET_PTR:
		case SQL_ATTR_ROW_NUMBER:
		case SQL_ATTR_ROW_OPERATION_PTR:
		case SQL_ATTR_SIMULATE_CURSOR:
		case SQL_ATTR_USE_BOOKMARKS:
		default:
			duckdb::DiagRecord diag_rec("Unsupported attribute type:" + std::to_string(attribute),
			                            SQLStateType::INVALID_ATTR_OPTION_ID, stmt->dbc->GetDataSourceName());
			throw duckdb::OdbcException("SQLSetStmtAttr", SQL_ERROR, diag_rec);
		}
	});
}

SQLRETURN SQL_API SQLPrepare(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	return duckdb::PrepareStmt(statement_handle, statement_text, text_length);
}

SQLRETURN SQL_API SQLCancel(SQLHSTMT statement_handle) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		stmt->dbc->conn->Interrupt();
		return SQL_SUCCESS;
	});
}

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	return duckdb::ExecDirectStmt(statement_handle, statement_text, text_length);
}

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqltables-function
SQLRETURN SQL_API SQLTables(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                            SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                            SQLSMALLINT name_length3, SQLCHAR *table_type, SQLSMALLINT name_length4) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN {
		auto catalog_n = OdbcUtils::ReadString(catalog_name, name_length1);
		string catalog_filter;
		if (catalog_n.empty()) {
			catalog_filter = "\"TABLE_CAT\" IS NULL";
		} else if (stmt->dbc->sql_attr_metadata_id == SQL_TRUE) {
			catalog_filter = "\"TABLE_CAT\"=" + OdbcUtils::GetStringAsIdentifier(catalog_n);
		}

		// String search pattern for schema name
		auto schema_n = OdbcUtils::ReadString(schema_name, name_length2);
		string schema_filter = OdbcUtils::ParseStringFilter("\"TABLE_SCHEM\"", schema_n,
		                                                    stmt->dbc->sql_attr_metadata_id, string(DEFAULT_SCHEMA));

		// String search pattern for table name
		auto table_n = OdbcUtils::ReadString(table_name, name_length3);
		string table_filter = OdbcUtils::ParseStringFilter("\"TABLE_NAME\"", table_n, stmt->dbc->sql_attr_metadata_id);

		auto table_tp = OdbcUtils::ReadString(table_type, name_length4);
		table_tp = std::regex_replace(table_tp, std::regex("('TABLE'|TABLE)"), "'BASE TABLE'");
		table_tp = std::regex_replace(table_tp, std::regex("('VIEW'|VIEW)"), "'VIEW'");
		table_tp = std::regex_replace(table_tp, std::regex("('%'|%)"), "'%'");

		// special cases
		if (catalog_n == std::string(SQL_ALL_CATALOGS) && name_length2 == 0 && name_length3 == 0 && name_length4 == 0) {
			if (!SQL_SUCCEEDED(
			        duckdb::ExecDirectStmt(statement_handle,
			                               (SQLCHAR *)"SELECT '' \"TABLE_CAT\", NULL \"TABLE_SCHEM\", NULL "
			                                          "\"TABLE_NAME\", NULL \"TABLE_TYPE\" , NULL \"REMARKS\"",
			                               SQL_NTS))) {
				return SQL_ERROR;
			}
			return SQL_SUCCESS;
		}

		if (schema_n == std::string(SQL_ALL_SCHEMAS) && catalog_n.empty() && name_length3 == 0) {
			if (!SQL_SUCCEEDED(duckdb::ExecDirectStmt(
			        statement_handle,
			        (SQLCHAR *)"SELECT '' \"TABLE_CAT\", schema_name \"TABLE_SCHEM\", NULL \"TABLE_NAME\", "
			                   "NULL \"TABLE_TYPE\" , NULL \"REMARKS\" FROM information_schema.schemata",
			        SQL_NTS))) {
				return SQL_ERROR;
			}
			return SQL_SUCCESS;
		}

		if (table_n == std::string(SQL_ALL_TABLE_TYPES) && name_length1 == 0 && name_length2 == 0 &&
		    name_length3 == 0) {
			if (!SQL_SUCCEEDED(duckdb::ExecDirectStmt(
			        statement_handle,
			        (SQLCHAR *)"SELECT * FROM (VALUES(NULL, NULL, NULL, 'TABLE'),(NULL, NULL, NULL, 'VIEW')) AS "
			                   "tbl(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE)",
			        SQL_NTS))) {
				return SQL_ERROR;
			}
			return SQL_SUCCESS;
		}

		string sql_tables = OdbcUtils::GetQueryDuckdbTables(schema_filter, table_filter, table_tp);

		if (!SQL_SUCCEEDED(
		        duckdb::ExecDirectStmt(statement_handle, (SQLCHAR *)sql_tables.c_str(), sql_tables.size()))) {
			return SQL_ERROR;
		}

		return SQL_SUCCESS;
	});
}

SQLRETURN SQL_API SQLColumns(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                             SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                             SQLSMALLINT name_length3, SQLCHAR *column_name, SQLSMALLINT name_length4) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN {
		// TODO see SQL_ATTR_METADATA_ID, parameters case
		auto catalog_n = OdbcUtils::ReadString(catalog_name, name_length1);
		string catalog_filter;
		if (catalog_n.empty()) {
			catalog_filter = "\"TABLE_CAT\" IS NULL";
		} else if (stmt->dbc->sql_attr_metadata_id == SQL_TRUE) {
			catalog_filter = "\"TABLE_CAT\"=" + OdbcUtils::GetStringAsIdentifier(catalog_n);
		}

		// String search pattern for schema name
		auto schema_n = OdbcUtils::ReadString(schema_name, name_length2);
		string schema_filter = OdbcUtils::ParseStringFilter("\"TABLE_SCHEM\"", schema_n,
		                                                    stmt->dbc->sql_attr_metadata_id, string(DEFAULT_SCHEMA));

		// String search pattern for table name
		auto table_n = OdbcUtils::ReadString(table_name, name_length3);
		string table_filter = OdbcUtils::ParseStringFilter("\"TABLE_NAME\"", table_n, stmt->dbc->sql_attr_metadata_id);

		// String search pattern for column name
		auto column_n = OdbcUtils::ReadString(column_name, name_length4);
		string column_filter =
		    OdbcUtils::ParseStringFilter("\"COLUMN_NAME\"", column_n, stmt->dbc->sql_attr_metadata_id);

		string sql_columns =
		    OdbcUtils::GetQueryDuckdbColumns(catalog_filter, schema_filter, table_filter, column_filter);

		auto ret = duckdb::ExecDirectStmt(statement_handle, (SQLCHAR *)sql_columns.c_str(), sql_columns.size());
		if (!SQL_SUCCEEDED(ret)) {
			return ret;
		}
		return SQL_SUCCESS;
	});
}

static SQLRETURN GetColAttribute(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
                                 SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length,
                                 SQLSMALLINT *string_length_ptr, SQLLEN *numeric_attribute_ptr) {

	return duckdb::WithStatementPrepared(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (column_number < 1 || column_number > stmt->stmt->GetTypes().size()) {
			duckdb::DiagRecord diag_rec("Column number out of range", SQLStateType::INVALID_DESC_INDEX,
			                            stmt->dbc->GetDataSourceName());
			throw duckdb::OdbcException("GetColAttribute", SQL_ERROR, diag_rec);
		}

		duckdb::idx_t col_idx = column_number - 1;
		auto desc_record = stmt->row_desc->GetIRD()->GetDescRecord(col_idx);

		switch (field_identifier) {
		case SQL_DESC_LABEL: {
			if (buffer_length <= 0) {
				duckdb::DiagRecord diag_rec("Inadequate buffer length", SQLStateType::INVALID_STR_BUFF_LENGTH,
				                            stmt->dbc->GetDataSourceName());
				throw duckdb::OdbcException("GetColAttribute", SQL_ERROR, diag_rec);
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
		case SQL_COLUMN_COUNT:
		case SQL_DESC_COUNT: {
			if (numeric_attribute_ptr) {
				duckdb::Store<SQLLEN>(stmt->row_desc->ird->header.sql_desc_count,
				                      (duckdb::data_ptr_t)numeric_attribute_ptr);
			}
			return SQL_SUCCESS;
		}
		case SQL_COLUMN_LENGTH:
		case SQL_DESC_LENGTH:
			if (numeric_attribute_ptr) {
				duckdb::Store<SQLLEN>(desc_record->sql_desc_length, (duckdb::data_ptr_t)numeric_attribute_ptr);
			}
			return SQL_SUCCESS;
		case SQL_DESC_OCTET_LENGTH:
			// 0 DuckDB doesn't provide octet length
			if (numeric_attribute_ptr) {
				*numeric_attribute_ptr = 0;
			}
			return SQL_SUCCESS;
		case SQL_DESC_TYPE_NAME: {
			if (buffer_length <= 0) {
				duckdb::DiagRecord diag_rec("Inadequate buffer length", SQLStateType::INVALID_STR_BUFF_LENGTH,
				                            stmt->dbc->GetDataSourceName());
				throw duckdb::OdbcException("GetColAttribute", SQL_ERROR, diag_rec);
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
			auto ret = duckdb::ApiInfo::GetColumnSize(stmt->stmt->GetTypes()[col_idx], numeric_attribute_ptr);
			if (ret == SQL_ERROR) {
				duckdb::DiagRecord diag_rec("Unsupported type for display size", SQLStateType::INVALID_PARAMETER_TYPE,
				                            stmt->dbc->GetDataSourceName());
				throw duckdb::OdbcException("GetColAttribute", SQL_ERROR, diag_rec);
			}
			return SQL_SUCCESS;
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
		case SQL_DESC_CONCISE_TYPE: {
			if (numeric_attribute_ptr) {
				duckdb::Store<SQLLEN>(desc_record->sql_desc_concise_type, (duckdb::data_ptr_t)numeric_attribute_ptr);
			}
			return SQL_SUCCESS;
		}
		case SQL_COLUMN_NULLABLE: {
			if (numeric_attribute_ptr) {
				duckdb::Store<SQLINTEGER>(desc_record->sql_desc_nullable, (duckdb::data_ptr_t)numeric_attribute_ptr);
			}
			return SQL_SUCCESS;
		}
		case SQL_DESC_NULLABLE: {
			if (numeric_attribute_ptr) {
				duckdb::Store<SQLLEN>(desc_record->sql_desc_nullable, (duckdb::data_ptr_t)numeric_attribute_ptr);
			}
			return SQL_SUCCESS;
		}
		case SQL_DESC_NUM_PREC_RADIX: {
			if (!numeric_attribute_ptr) {
				return SQL_SUCCESS;
			}
			switch (desc_record->sql_desc_type) {
			case SQL_TINYINT:
			case SQL_SMALLINT:
			case SQL_INTEGER:
			case SQL_BIGINT:
				// 10 for exact numeric data type
				duckdb::Store<SQLLEN>(10, (duckdb::data_ptr_t)numeric_attribute_ptr);
				break;
			case SQL_FLOAT:
			case SQL_DOUBLE:
			case SQL_NUMERIC:
				// 2 for an approximate numeric data type
				duckdb::Store<SQLLEN>(2, (duckdb::data_ptr_t)numeric_attribute_ptr);
				break;
			default:
				// set to 0 for all non-numeric data type
				duckdb::Store<SQLLEN>(0, (duckdb::data_ptr_t)numeric_attribute_ptr);
				break;
			}
			return SQL_SUCCESS;
		}
		case SQL_COLUMN_SCALE: {
			if (numeric_attribute_ptr) {
				duckdb::Store<SQLINTEGER>(desc_record->sql_desc_scale, (duckdb::data_ptr_t)numeric_attribute_ptr);
			}
			return SQL_SUCCESS;
		}
		case SQL_DESC_SCALE: {
			if (numeric_attribute_ptr) {
				duckdb::Store<SQLLEN>(desc_record->sql_desc_scale, (duckdb::data_ptr_t)numeric_attribute_ptr);
			}
			return SQL_SUCCESS;
		}
		case SQL_COLUMN_PRECISION: {
			if (numeric_attribute_ptr) {
				duckdb::Store<SQLINTEGER>(desc_record->sql_desc_precision, (duckdb::data_ptr_t)numeric_attribute_ptr);
			}
			return SQL_SUCCESS;
		}
		case SQL_DESC_PRECISION: {
			if (numeric_attribute_ptr) {
				duckdb::Store<SQLLEN>(desc_record->sql_desc_precision, (duckdb::data_ptr_t)numeric_attribute_ptr);
			}
			return SQL_SUCCESS;
		}
		case SQL_COLUMN_NAME:
		case SQL_DESC_NAME: {
			duckdb::OdbcUtils::WriteString(desc_record->sql_desc_name, (SQLCHAR *)character_attribute_ptr,
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_UPDATABLE: {
			if (numeric_attribute_ptr) {
				duckdb::Store<SQLSMALLINT>(desc_record->sql_desc_updatable, (duckdb::data_ptr_t)numeric_attribute_ptr);
			}
			return SQL_SUCCESS;
		}
		default:
			duckdb::DiagRecord diag_rec("Unsupported attribute type", SQLStateType::INVALID_ATTR_OPTION_ID,
			                            stmt->dbc->GetDataSourceName());
			throw duckdb::OdbcException("GetColAttribute", SQL_ERROR, diag_rec);
		}
	});
}

SQLRETURN SQL_API SQLColAttributes(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
                                   SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length,
                                   SQLSMALLINT *string_length_ptr, SQLLEN *numeric_attribute_ptr) {
	return GetColAttribute(statement_handle, column_number, field_identifier, character_attribute_ptr, buffer_length,
	                       string_length_ptr, numeric_attribute_ptr);
}

SQLRETURN SQL_API SQLColAttribute(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
                                  SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length,
                                  SQLSMALLINT *string_length_ptr, SQLLEN *numeric_attribute_ptr) {
	return GetColAttribute(statement_handle, column_number, field_identifier, character_attribute_ptr, buffer_length,
	                       string_length_ptr, numeric_attribute_ptr);
}

SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT statement_handle, SQLUSMALLINT option) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN {
		if (option == SQL_DROP) {
			// mapping FreeStmt with DROP option to SQLFreeHandle
			return duckdb::FreeHandle(SQL_HANDLE_STMT, statement_handle);
		}
		if (option == SQL_UNBIND) {
			stmt->bound_cols.clear();
			return SQL_SUCCESS;
		}
		if (option == SQL_RESET_PARAMS) {
			stmt->param_desc->Clear();
			return SQL_SUCCESS;
		}
		if (option == SQL_CLOSE) {
			return duckdb::CloseStmt(stmt);
		}
		return SQL_ERROR;
	});
}

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT statement_handle) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN {
		if (!stmt->param_desc->HasParamSetToProcess()) {
			return SQL_NO_DATA;
		}
		return duckdb::SingleExecuteStmt(stmt);
	});
}
