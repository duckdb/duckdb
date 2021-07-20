#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "api_info.hpp"

SQLRETURN SQLSetStmtAttr(SQLHSTMT statement_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                         SQLINTEGER string_length) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (!value_ptr) {
			return SQL_ERROR;
		}
		switch (attribute) {
		case SQL_ATTR_PARAMSET_SIZE: {
			/* auto size = Load<SQLLEN>((data_ptr_t) value_ptr);
			 return (size == 1) ? SQL_SUCCESS : SQL_ERROR;
			 */
			// this should be 1
			return SQL_SUCCESS;
		}
		case SQL_ATTR_QUERY_TIMEOUT: {
			// this should be 0
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROW_ARRAY_SIZE: {
			// this should be 1 (for now!)
			// TODO allow fetch to put more rows in bound cols
			auto new_size = (SQLULEN)value_ptr;
			if (new_size != 1) {
				return SQL_ERROR;
			}
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROWS_FETCHED_PTR: {
			stmt->rows_fetched_ptr = (SQLULEN *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROW_BIND_TYPE: {
			if (value_ptr && (uint64_t)value_ptr != SQL_BIND_BY_COLUMN) {
				//! it's a row-wise binding orientation (SQLFetch should support it)
				stmt->row_length = value_ptr;
				stmt->row_wise = true;
			}
			return SQL_SUCCESS;
		}
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
	auto prepare_status = duckdb::PrepareStmt(statement_handle, statement_text, text_length);
	if (prepare_status != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	auto execute_status = duckdb::ExecuteStmt(statement_handle);
	if (execute_status != SQL_SUCCESS) {
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
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
		if (!SQL_SUCCEEDED(SQLExecDirect(statement_handle,
		                                 (SQLCHAR *)"SELECT '' \"TABLE_CAT\", NULL \"TABLE_SCHEM\", NULL "
		                                            "\"TABLE_NAME\", NULL \"TABLE_TYPE\" , NULL \"REMARKS\"",
		                                 SQL_NTS))) {
			return SQL_ERROR;
		}
		return SQL_SUCCESS;
	}

	if (schema_n == std::string(SQL_ALL_SCHEMAS) && name_length1 == 0 && name_length3 == 0) {
		if (!SQL_SUCCEEDED(
		        SQLExecDirect(statement_handle,
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

	if (!SQL_SUCCEEDED(SQLPrepare(
	        statement_handle,
	        (SQLCHAR
	             *)"SELECT table_catalog \"TABLE_CAT\", table_schema \"TABLE_SCHEM\", table_name \"TABLE_NAME\", CASE "
	               "WHEN table_type='BASE TABLE' THEN 'TABLE' ELSE table_type END \"TABLE_TYPE\" , '' \"REMARKS\"  "
	               "FROM information_schema.tables WHERE table_schema LIKE ? AND table_name LIKE ? and table_type = ?",
	        SQL_NTS))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLBindParameter(statement_handle, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0, 0,
	                                    schema_name, name_length2, nullptr))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLBindParameter(statement_handle, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0, 0,
	                                    table_name, name_length3, nullptr))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLBindParameter(statement_handle, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0, 0,
	                                    table_type, name_length4, nullptr))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLExecute(statement_handle))) {
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
			auto out_len = snprintf((char *)character_attribute_ptr, buffer_length, "%s", col_name.c_str());
			if (string_length_ptr) {
				*string_length_ptr = out_len;
			}

			return SQL_SUCCESS;
		}
		case SQL_DESC_OCTET_LENGTH:
			// 0 DuckDB doesn't provide octet length
			*numeric_attribute_ptr = 0;
			return SQL_SUCCESS;
		case SQL_DESC_TYPE_NAME: {
			if (buffer_length <= 0) {
				stmt->error_messages.emplace_back("Inadequate buffer length.");
				return SQL_ERROR;
			}

			auto internal_type = stmt->stmt->GetTypes()[col_idx].InternalType();
			std::string type_name = duckdb::TypeIdToString(internal_type);
			auto out_len = snprintf((char *)character_attribute_ptr, buffer_length, "%s", type_name.c_str());
			if (string_length_ptr) {
				*string_length_ptr = out_len;
			}

			return SQL_SUCCESS;
		}
		// https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/display-size?view=sql-server-ver15
		case SQL_DESC_DISPLAY_SIZE: {
			auto logical_type = stmt->stmt->GetTypes()[col_idx];
			auto sql_type = duckdb::ApiInfo::FindRelatedSQLType(logical_type.id());
			switch (sql_type) {
			case SQL_DECIMAL:
			case SQL_NUMERIC:
				*numeric_attribute_ptr =
				    duckdb::DecimalType::GetWidth(logical_type) + duckdb::DecimalType::GetScale(logical_type);
				return SQL_SUCCESS;
			case SQL_BIT:
				*numeric_attribute_ptr = 1;
				return SQL_SUCCESS;
			case SQL_TINYINT:
				*numeric_attribute_ptr = 6;
				return SQL_SUCCESS;
			case SQL_INTEGER:
				*numeric_attribute_ptr = 11;
				return SQL_SUCCESS;
			case SQL_BIGINT:
				*numeric_attribute_ptr = 20;
				return SQL_SUCCESS;
			case SQL_REAL:
				*numeric_attribute_ptr = 14;
				return SQL_SUCCESS;
			case SQL_FLOAT:
			case SQL_DOUBLE:
				*numeric_attribute_ptr = 24;
				return SQL_SUCCESS;
			case SQL_TYPE_DATE:
				*numeric_attribute_ptr = 10;
				return SQL_SUCCESS;
			case SQL_TYPE_TIME:
				*numeric_attribute_ptr = 9;
				return SQL_SUCCESS;
			case SQL_TYPE_TIMESTAMP:
				*numeric_attribute_ptr = 20;
				return SQL_SUCCESS;
			case SQL_VARCHAR:
			case SQL_VARBINARY:
				// we don't know the number of characters
				*numeric_attribute_ptr = 0;
				return SQL_SUCCESS;
			default:
				stmt->error_messages.emplace_back("Unsupported type for display size.");
				return SQL_ERROR;
			}
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
			stmt->params.clear();
			return SQL_SUCCESS;
		}
		if (option == SQL_CLOSE) {
			stmt->res.reset();
			stmt->chunk.reset();
			// stmt->stmt.reset(); // the statment can be reuse in prepared statement
			stmt->bound_cols.clear();
			stmt->params.clear();
			stmt->error_messages.clear();
			return SQL_SUCCESS;
		}
		return SQL_ERROR;
	});
}
