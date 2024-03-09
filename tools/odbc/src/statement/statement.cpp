#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "driver.hpp"
#include "odbc_diagnostic.hpp"
#include "odbc_fetch.hpp"
#include "odbc_utils.hpp"
#include "parameter_descriptor.hpp"
#include "row_descriptor.hpp"
#include "statement_functions.hpp"
#include "handle_functions.hpp"

#include "duckdb/common/constants.hpp"

#include <regex>

using duckdb::LogicalTypeId;
using duckdb::OdbcUtils;
using duckdb::SQLStateType;
using std::string;

/**
 * @brief SQLSetStmtAttr sets attributes that govern aspects of a statement.
 * @param statement_handle
 * @param attribute The attribute to set. See
 * https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetstmtattr-function
 * @param value_ptr The value to set the attribute to.  Depending of the value of attribute, it can be:
 * 					\n- ODBC descriptor handle
 * 					\n- SQLUINTEGER value
 * 					\n- SQLULEN value
 * 					\n- a pointer to: null-terminated character string, a bin, a value or array of type SQLLEN, SQLULEN,
 * or SQLUSMALLINT, a driver-defined value
 * @param string_length The length of the value_ptr parameter.  See
 * https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetstmtattr-function for details.
 * @return SQL return code
 */
SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                 SQLINTEGER string_length) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	switch (attribute) {
	case SQL_ATTR_PARAMSET_SIZE: {
		hstmt->param_desc->apd->header.sql_desc_array_size = (SQLULEN)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAM_BIND_TYPE: {
		if (value_ptr) {
			hstmt->param_desc->apd->header.sql_desc_bind_type = (SQLINTEGER)(SQLULEN)(uintptr_t)value_ptr;
		}
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAMS_PROCESSED_PTR: {
		hstmt->param_desc->SetParamProcessedPtr((SQLULEN *)value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAM_STATUS_PTR: {
		hstmt->param_desc->SetArrayStatusPtr((SQLUSMALLINT *)value_ptr);
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
			hstmt->row_desc->ard->header.sql_desc_array_size = new_size;
		}
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROWS_FETCHED_PTR: {
		hstmt->rows_fetched_ptr = (SQLULEN *)value_ptr;
		hstmt->row_desc->ird->header.sql_desc_rows_processed_ptr = (SQLULEN *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_BIND_TYPE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		hstmt->row_desc->ard->header.sql_desc_bind_type = (SQLULEN)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_STATUS_PTR: {
		hstmt->row_desc->ird->header.sql_desc_array_status_ptr = (SQLUSMALLINT *)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CURSOR_TYPE: {
		hstmt->odbc_fetcher->cursor_type = (SQLULEN)value_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_APP_ROW_DESC: {
		hstmt->SetARD((duckdb::OdbcHandleDesc *)value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_APP_PARAM_DESC: {
		hstmt->SetAPD((duckdb::OdbcHandleDesc *)value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_IMP_PARAM_DESC:
	case SQL_ATTR_IMP_ROW_DESC: {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLSetStmtAttr",
		                                   "Option value changed:" + std::to_string(attribute), SQLStateType::ST_HY017,
		                                   hstmt->dbc->GetDataSourceName());
	}
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR: {
		hstmt->param_desc->SetBindOffesetPtr((SQLLEN *)value_ptr);
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CONCURRENCY: {
		SQLULEN value = (SQLULEN)(uintptr_t)value_ptr;
		if (value != SQL_CONCUR_LOCK) {
			return duckdb::SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLSetStmtAttr",
			                                   "Option value changed:" + std::to_string(attribute),
			                                   SQLStateType::ST_01S02, hstmt->dbc->GetDataSourceName());
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
			return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLSetStmtAttr",
			                                   "Invalid attribute value: " + std::to_string(attribute),
			                                   SQLStateType::ST_HY024, hstmt->dbc->GetDataSourceName());
		}
		hstmt->retrieve_data = value;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CURSOR_SCROLLABLE: {
		SQLULEN value = (SQLULEN)(uintptr_t)value_ptr;
		switch (value) {
		case SQL_NONSCROLLABLE:
			hstmt->odbc_fetcher->cursor_type = SQL_CURSOR_FORWARD_ONLY;
			break;
		case SQL_SCROLLABLE:
			hstmt->odbc_fetcher->cursor_type = SQL_CURSOR_STATIC;
			break;
		default:
			return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLSetStmtAttr",
			                                   "Invalid attribute value:" + std::to_string(attribute),
			                                   SQLStateType::ST_HY024, hstmt->dbc->GetDataSourceName());
		}
		hstmt->odbc_fetcher->cursor_scrollable = value;
		return SQL_SUCCESS;
	}
	default:
		return duckdb::SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLSetStmtAttr",
		                                   "Option value changed:" + std::to_string(attribute), SQLStateType::ST_01S02,
		                                   hstmt->dbc->GetDataSourceName());
	}

	return SQL_SUCCESS;
}

/**
 * @brief SQLGetStmtAttr returns attributes that govern aspects of a statement.
 * @param statement_handle
 * @param attribute [INPUT] The attribute to set
 * @param value_ptr [OUTPUT] The value to set the attribute to.  Depending of the value of attribute, it can be:
 * 					\n- ODBC descriptor handle
 * 					\n- SQLUINTEGER value
 * 					\n- SQLULEN value
 * 					\n- a pointer to: null-terminated character string, a bin, a value or array of type SQLLEN, SQLULEN,
 * or SQLUSMALLINT, a driver-defined value
 * @param buffer_length
 * @param string_length_ptr
 * @return
 */
SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                 SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	switch (attribute) {
	case SQL_ATTR_APP_PARAM_DESC:
	case SQL_ATTR_IMP_PARAM_DESC:
	case SQL_ATTR_APP_ROW_DESC:
	case SQL_ATTR_IMP_ROW_DESC: {
		if (string_length_ptr) {
			*string_length_ptr = SQL_IS_POINTER;
		}
		SQLHDESC *desc_handle = reinterpret_cast<SQLHDESC *>(value_ptr);
		if (attribute == SQL_ATTR_APP_PARAM_DESC) {
			*desc_handle = hstmt->param_desc->GetAPD();
		}
		if (attribute == SQL_ATTR_IMP_PARAM_DESC) {
			*desc_handle = hstmt->param_desc->GetIPD();
		}
		if (attribute == SQL_ATTR_APP_ROW_DESC) {
			*desc_handle = hstmt->row_desc->GetARD();
		}
		if (attribute == SQL_ATTR_IMP_ROW_DESC) {
			*desc_handle = hstmt->row_desc->GetIRD();
		}
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*((SQLLEN **)value_ptr) = hstmt->param_desc->GetBindOffesetPtr();
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAM_BIND_TYPE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*((SQLINTEGER *)value_ptr) = hstmt->param_desc->apd->header.sql_desc_bind_type;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAMS_PROCESSED_PTR: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLULEN **)value_ptr = hstmt->param_desc->GetParamProcessedPtr();
		return SQL_SUCCESS;
	}
	case SQL_ATTR_PARAMSET_SIZE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLULEN *)value_ptr = hstmt->param_desc->apd->header.sql_desc_array_size;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_ARRAY_SIZE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLULEN *)value_ptr = hstmt->row_desc->ard->header.sql_desc_array_size;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROWS_FETCHED_PTR: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLULEN **)value_ptr = hstmt->rows_fetched_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_BIND_TYPE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		if ((SQLULEN)value_ptr != SQL_BIND_BY_COLUMN) {
			//! it's a row-wise binding orientation
			*(SQLULEN *)value_ptr = hstmt->row_desc->ard->header.sql_desc_bind_type;
			return SQL_SUCCESS;
		}
		*(SQLULEN *)value_ptr = SQL_BIND_BY_COLUMN;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_STATUS_PTR: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLUSMALLINT **)value_ptr = hstmt->row_desc->ird->header.sql_desc_array_status_ptr;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CURSOR_TYPE: {
		if (value_ptr == nullptr) {
			return SQL_ERROR;
		}
		*(SQLULEN *)value_ptr = hstmt->odbc_fetcher->cursor_type;
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
		*((SQLULEN *)value_ptr) = hstmt->retrieve_data;
		return SQL_SUCCESS;
	}
	case SQL_ATTR_CURSOR_SCROLLABLE: {
		*((SQLULEN *)value_ptr) = hstmt->odbc_fetcher->cursor_scrollable;
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
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLSetStmtAttr",
		                                   "Unsupported attribute type:" + std::to_string(attribute),
		                                   SQLStateType::ST_HY092, hstmt->dbc->GetDataSourceName());
	}
}

/**
 * @brief Prepares an SQL string for execution
 * https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlprepare-function?view=sql-server-ver16
 * @param statement_handle
 * @param statement_text SQL string to be prepared
 * @param text_length The length of the statement text in characters
 * @return
 */
SQLRETURN SQL_API SQLPrepare(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	return duckdb::PrepareStmt(statement_handle, statement_text, text_length);
}

/**
 * @brief Cancels the processing on a statement
 * @param statement_handle
 * @return
 */
SQLRETURN SQL_API SQLCancel(SQLHSTMT statement_handle) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	hstmt->dbc->conn->Interrupt();
	return SQL_SUCCESS;
}

/**
 *@brief  Executes a prepared statement
 * https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlexecdirect-function?view=sql-server-ver15
 * @param statement_handle A handle to a statement object. Stores information about the statement and the results of a
 *query.
 * @param statement_text The text of the query to execute.
 * @param text_length The length of the query text in characters.
 * @return
 */
SQLRETURN SQL_API SQLExecDirect(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	return duckdb::ExecDirectStmt(statement_handle, statement_text, text_length);
}

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqltables-function
SQLRETURN SQL_API SQLTables(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                            SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                            SQLSMALLINT name_length3, SQLCHAR *table_type, SQLSMALLINT name_length4) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	auto catalog_n = OdbcUtils::ReadString(catalog_name, name_length1);
	string catalog_filter;
	if (catalog_n.empty()) {
		catalog_filter = "\"TABLE_CAT\" IS NULL";
	} else if (hstmt->dbc->sql_attr_metadata_id == SQL_TRUE) {
		catalog_filter = "\"TABLE_CAT\"=" + OdbcUtils::GetStringAsIdentifier(catalog_n);
	}

	// String search pattern for schema name
	auto schema_n = OdbcUtils::ReadString(schema_name, name_length2);
	string schema_filter = OdbcUtils::ParseStringFilter("\"TABLE_SCHEM\"", schema_n, hstmt->dbc->sql_attr_metadata_id,
	                                                    string(DEFAULT_SCHEMA));

	// String search pattern for table name
	auto table_n = OdbcUtils::ReadString(table_name, name_length3);
	string table_filter = OdbcUtils::ParseStringFilter("\"TABLE_NAME\"", table_n, hstmt->dbc->sql_attr_metadata_id);

	auto table_tp = OdbcUtils::ReadString(table_type, name_length4);

	// .Net ODBC driver GetSchema() call includes "SYSTEM TABLE" (doesn't exist in DuckDB),
	// and the regex tweaks below break if "SYSTEM TABLE" is in the query, so remove it
	table_tp = std::regex_replace(table_tp, std::regex("('SYSTEM TABLE'|SYSTEM TABLE)"), "");

	table_tp = std::regex_replace(table_tp, std::regex("('TABLE'|TABLE)"), "'BASE TABLE'");
	table_tp = std::regex_replace(table_tp, std::regex("('VIEW'|VIEW)"), "'VIEW'");
	table_tp = std::regex_replace(table_tp, std::regex("('%'|%)"), "'%'");

	// special cases
	if (catalog_n == std::string(SQL_ALL_CATALOGS) && name_length2 == 0 && name_length3 == 0 && name_length4 == 0) {
		if (!SQL_SUCCEEDED(duckdb::ExecDirectStmt(statement_handle,
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

	if (table_n == std::string(SQL_ALL_TABLE_TYPES) && name_length1 == 0 && name_length2 == 0 && name_length3 == 0) {
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

	if (!SQL_SUCCEEDED(duckdb::ExecDirectStmt(statement_handle, (SQLCHAR *)sql_tables.c_str(), sql_tables.size()))) {
		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLColumns(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                             SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                             SQLSMALLINT name_length3, SQLCHAR *column_name, SQLSMALLINT name_length4) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	auto catalog_n = OdbcUtils::ReadString(catalog_name, name_length1);
	string catalog_filter;
	if (catalog_n.empty()) {
		catalog_filter = "\"TABLE_CAT\" IS NULL";
	} else if (hstmt->dbc->sql_attr_metadata_id == SQL_TRUE) {
		catalog_filter = "\"TABLE_CAT\"=" + OdbcUtils::GetStringAsIdentifier(catalog_n);
	}

	// String search pattern for schema name
	auto schema_n = OdbcUtils::ReadString(schema_name, name_length2);
	string schema_filter = OdbcUtils::ParseStringFilter("\"TABLE_SCHEM\"", schema_n, hstmt->dbc->sql_attr_metadata_id,
	                                                    string(DEFAULT_SCHEMA));

	// String search pattern for table name
	auto table_n = OdbcUtils::ReadString(table_name, name_length3);
	string table_filter = OdbcUtils::ParseStringFilter("\"TABLE_NAME\"", table_n, hstmt->dbc->sql_attr_metadata_id);

	// String search pattern for column name
	auto column_n = OdbcUtils::ReadString(column_name, name_length4);
	string column_filter = OdbcUtils::ParseStringFilter("\"COLUMN_NAME\"", column_n, hstmt->dbc->sql_attr_metadata_id);

	string sql_columns = OdbcUtils::GetQueryDuckdbColumns(catalog_filter, schema_filter, table_filter, column_filter);

	ret = duckdb::ExecDirectStmt(statement_handle, (SQLCHAR *)sql_columns.c_str(), sql_columns.size());
	if (!SQL_SUCCEEDED(ret)) {
		return ret;
	}
	return SQL_SUCCESS;
}

template <typename T>
static SQLRETURN SetNumericAttributePtr(duckdb::OdbcHandleStmt *hstmt, const T &val, SQLLEN *numeric_attribute_ptr) {
	if (numeric_attribute_ptr) {
		duckdb::Store<SQLLEN>(val, reinterpret_cast<duckdb::data_ptr_t>(numeric_attribute_ptr));
		return SQL_SUCCESS;
	}
	return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)",
	                                   "Memory management error; numeric "
	                                   "attribute pointer is null",
	                                   SQLStateType::ST_HY013, hstmt->dbc->GetDataSourceName());
}

static SQLRETURN SetCharacterAttributePtr(duckdb::OdbcHandleStmt *hstmt, const string &str,
                                          SQLCHAR *character_attribute_ptr, SQLSMALLINT buffer_length,
                                          SQLSMALLINT *string_length_ptr) {
	if (buffer_length <= 0) {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)", "Invalid string or buffer length",
		                                   SQLStateType::ST_HY090, hstmt->dbc->GetDataSourceName());
	}
	if (character_attribute_ptr) {
		duckdb::OdbcUtils::WriteString(str, character_attribute_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)",
	                                   "Memory management error; character "
	                                   "attribute pointer is null",
	                                   SQLStateType::ST_HY013, hstmt->dbc->GetDataSourceName());
}

static SQLRETURN GetColAttribute(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
                                 SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length,
                                 SQLSMALLINT *string_length_ptr, SQLLEN *numeric_attribute_ptr) {

	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMTPrepared(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (field_identifier != SQL_DESC_COUNT && hstmt->res &&
	    hstmt->res->properties.return_type != duckdb::StatementReturnType::QUERY_RESULT) {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)",
		                                   "Prepared statement not a cursor-specification", SQLStateType::ST_07005,
		                                   hstmt->dbc->GetDataSourceName());
	}

	// TODO: SQL_DESC_TYPE and SQL_DESC_OCTET_LENGTH should return values if column_number is 0, otherwise they should
	// return undefined values
	if (column_number < 1 || column_number > hstmt->stmt->GetTypes().size()) {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)", "Invalid descriptor index",
		                                   SQLStateType::ST_07009, hstmt->dbc->GetDataSourceName());
	}

	duckdb::idx_t col_idx = column_number - 1;
	auto desc_record = hstmt->row_desc->GetIRD()->GetDescRecord(col_idx);

	switch (field_identifier) {
	case SQL_DESC_AUTO_UNIQUE_VALUE: {
		ret = SetNumericAttributePtr(hstmt, desc_record->sql_desc_auto_unique_value, numeric_attribute_ptr);
		if (ret == SQL_ERROR) {
			return ret;
		}
		return duckdb::SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLColAttribute(s)",
		                                   "DuckDB does not support autoincrementing columns", SQLStateType::ST_HYC00,
		                                   hstmt->dbc->GetDataSourceName());
	}
	case SQL_DESC_BASE_COLUMN_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_base_column_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_TABLE_NAME:
	case SQL_DESC_BASE_TABLE_NAME: {
		// not possible to access this information (also not possible in HandleStmt::FillIRD)
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)", "Driver not capable",
		                                   SQLStateType::ST_HYC00, hstmt->dbc->GetDataSourceName());
	}
	case SQL_DESC_CASE_SENSITIVE:
		// DuckDB is case insensitive so will always return false
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_case_sensitive, numeric_attribute_ptr);
	case SQL_DESC_CATALOG_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_catalog_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_CONCISE_TYPE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_concise_type, numeric_attribute_ptr);
	case SQL_COLUMN_COUNT:
	case SQL_DESC_COUNT:
		return SetNumericAttributePtr(hstmt, hstmt->row_desc->GetIRD()->GetRecordCount(), numeric_attribute_ptr);
	case SQL_DESC_DISPLAY_SIZE: {
		ret = SetNumericAttributePtr(hstmt, desc_record->sql_desc_display_size, numeric_attribute_ptr);
		if (desc_record->sql_desc_display_size == 0) {
			return duckdb::SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLColAttribute(s)",
			                                   "Cannot determine the column or parameter length of variable types",
			                                   SQLStateType::ST_01000, hstmt->dbc->GetDataSourceName());
		}
		return ret;
	}
	case SQL_DESC_FIXED_PREC_SCALE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_fixed_prec_scale, numeric_attribute_ptr);
	case SQL_DESC_LABEL:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_label,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_COLUMN_LENGTH:
	case SQL_DESC_LENGTH:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_length, numeric_attribute_ptr);
	case SQL_DESC_LITERAL_PREFIX:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_literal_prefix,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_LITERAL_SUFFIX:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_literal_suffix,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_LOCAL_TYPE_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_local_type_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_COLUMN_NAME:
	case SQL_DESC_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_COLUMN_NULLABLE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_nullable, numeric_attribute_ptr);
	case SQL_DESC_NULLABLE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_nullable, numeric_attribute_ptr);
	case SQL_DESC_NUM_PREC_RADIX:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_num_prec_radix, numeric_attribute_ptr);
	case SQL_DESC_OCTET_LENGTH:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_octet_length, numeric_attribute_ptr);
	case SQL_COLUMN_PRECISION:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_precision, numeric_attribute_ptr);
	case SQL_DESC_PRECISION:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_precision, numeric_attribute_ptr);
	case SQL_COLUMN_SCALE:
	case SQL_DESC_SCALE: {
		if (desc_record->sql_desc_scale == -1) {
			ret = SetNumericAttributePtr(hstmt, SQL_NO_TOTAL, numeric_attribute_ptr);
			if (SQL_SUCCEEDED(ret)) {
				return duckdb::SetDiagnosticRecord(hstmt, SQL_SUCCESS_WITH_INFO, "SQLColAttribute(s)",
				                                   "SQL_NO_TOTAL: Scale is undefined for this column data type",
				                                   SQLStateType::ST_01000, hstmt->dbc->GetDataSourceName());
			}
			return ret;
		}
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_scale, numeric_attribute_ptr);
	}
	case SQL_DESC_SCHEMA_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_schema_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_SEARCHABLE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_searchable, numeric_attribute_ptr);
	case SQL_DESC_TYPE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_type, numeric_attribute_ptr);
	case SQL_DESC_TYPE_NAME:
		return SetCharacterAttributePtr(hstmt, desc_record->sql_desc_type_name,
		                                static_cast<SQLCHAR *>(character_attribute_ptr), buffer_length,
		                                string_length_ptr);
	case SQL_DESC_UNNAMED:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_unnamed, numeric_attribute_ptr);
	case SQL_DESC_UNSIGNED:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_unsigned, numeric_attribute_ptr);
	case SQL_DESC_UPDATABLE:
		return SetNumericAttributePtr(hstmt, desc_record->sql_desc_updatable, numeric_attribute_ptr);
	default: {
		return duckdb::SetDiagnosticRecord(hstmt, SQL_ERROR, "SQLColAttribute(s)",
		                                   "Invalid descriptor field identifier", SQLStateType::ST_HY091,
		                                   hstmt->dbc->GetDataSourceName());
	}
	}
}

SQLRETURN SQL_API SQLColAttributes(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
                                   SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length,
                                   SQLSMALLINT *string_length_ptr, SQLLEN *numeric_attribute_ptr) {
	return GetColAttribute(statement_handle, column_number, field_identifier, character_attribute_ptr, buffer_length,
	                       string_length_ptr, numeric_attribute_ptr);
}

/**
 * @brief Returns description of a column in a result set
 * https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16
 * @param statement_handle
 * @param column_number [INPUT] The column number for which to return information. Column numbers start at 1.
 * @param field_identifier [INPUT] The description field to return.
 * @param character_attribute_ptr [OUTPUT] A pointer to a buffer in which to return the value of the requested
 * descriptor field, if the field is a string. If the ptr is NULL, string_length_ptr return the total number of
 * characters available to return.
 * @param buffer_length [OUTPUT] Optional parameter that specifies the length of the character_attribute_ptr buffer. If
 * the field is a string, the buffer_length parameter must be large enough to hold the entire string, including the
 * null-termination character.
 * @param string_length_ptr [OUTPUT] A pointer to a buffer in which to return the number of characters (excluding the
 * null-termination character) available to return in character_attribute_ptr.
 * @param numeric_attribute_ptr [OUTPUT] A pointer to a buffer in which to return the value of the requested descriptor
 * field, if the field is a numeric value.
 * @return
 */
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLUSMALLINT field_identifier,
                                  SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length,
                                  SQLSMALLINT *string_length_ptr, SQLLEN *numeric_attribute_ptr) {
	return GetColAttribute(statement_handle, column_number, field_identifier, character_attribute_ptr, buffer_length,
	                       string_length_ptr, numeric_attribute_ptr);
}

/**
 * @brief Stops processing associated with a specific statement, closes any open cursors associated with the statement,
 * discards pending results, or, optionally, frees all resources associated with the statement handle.
 * https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfreestmt-function?view=sql-server-ver16
 * @param statement_handle
 * @param option SQL_CLOSE, SQL_DROP, SQL_UNBIND, SQL_RESET_PARAMS
 * @return
 */
SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT statement_handle, SQLUSMALLINT option) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (option == SQL_DROP) {
		// mapping FreeStmt with DROP option to SQLFreeHandle
		return duckdb::FreeHandle(SQL_HANDLE_STMT, statement_handle);
	}
	if (option == SQL_UNBIND) {
		hstmt->bound_cols.clear();
		return SQL_SUCCESS;
	}
	if (option == SQL_RESET_PARAMS) {
		hstmt->param_desc->Clear();
		return SQL_SUCCESS;
	}
	if (option == SQL_CLOSE) {
		return duckdb::CloseStmt(hstmt);
	}
	return SQL_ERROR;
}

/**
 * @brief Determines whether more results are available on a statement, and, if so, prepares the next result set.
 * https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlmoreresults-function?view=sql-server-ver15
 * @param statement_handle
 * @return
 */
SQLRETURN SQL_API SQLMoreResults(SQLHSTMT statement_handle) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	SQLRETURN ret = ConvertHSTMT(statement_handle, hstmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	if (!hstmt->param_desc->HasParamSetToProcess()) {
		return SQL_NO_DATA;
	}
	return duckdb::SingleExecuteStmt(hstmt);
}
