#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"

using namespace duckdb;

SQLRETURN SQLSetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!ValuePtr) {
			return SQL_ERROR;
		}
		switch (Attribute) {
		case SQL_ATTR_PARAMSET_SIZE: {
			/* auto size = Load<SQLLEN>((data_ptr_t) ValuePtr);
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
			auto new_size = (SQLULEN)ValuePtr;
			if (new_size != 1) {
				return SQL_ERROR;
			}
			return SQL_SUCCESS;
		}
		case SQL_ATTR_ROWS_FETCHED_PTR: {
			stmt->rows_fetched_ptr = (SQLULEN *)ValuePtr;
			return SQL_SUCCESS;
		}
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQLPrepare(SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength) {
	return PrepareStmt(StatementHandle, StatementText, TextLength);
}

SQLRETURN SQLCancel(SQLHSTMT StatementHandle) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) {
		stmt->dbc->conn->Interrupt();
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLExecDirect(SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength) {
	auto prepare_status = PrepareStmt(StatementHandle, StatementText, TextLength);
	if (prepare_status != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	auto execute_status = ExecuteStmt(StatementHandle);
	if (execute_status != SQL_SUCCESS) {
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
}

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqltables-function
SQLRETURN SQLTables(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1, SQLCHAR *SchemaName,
                    SQLSMALLINT NameLength2, SQLCHAR *TableName, SQLSMALLINT NameLength3, SQLCHAR *TableType,
                    SQLSMALLINT NameLength4) {

	auto catalog_name = OdbcUtils::ReadString(CatalogName, NameLength1);
	auto schema_name = OdbcUtils::ReadString(SchemaName, NameLength2);
	auto table_name = OdbcUtils::ReadString(TableName, NameLength3);
	auto table_type = OdbcUtils::ReadString(TableType, NameLength4);

	// special cases
	if (catalog_name == string(SQL_ALL_CATALOGS) && NameLength2 == 0 && NameLength2 == 0) {
		if (!SQL_SUCCEEDED(SQLExecDirect(StatementHandle,
		                                 (SQLCHAR *)"SELECT '' \"TABLE_CAT\", NULL \"TABLE_SCHEM\", NULL "
		                                            "\"TABLE_NAME\", NULL \"TABLE_TYPE\" , NULL \"REMARKS\"",
		                                 SQL_NTS))) {
			return SQL_ERROR;
		}
		return SQL_SUCCESS;
	}

	if (schema_name == string(SQL_ALL_SCHEMAS) && NameLength1 == 0 && NameLength3 == 0) {
		if (!SQL_SUCCEEDED(
		        SQLExecDirect(StatementHandle,
		                      (SQLCHAR *)"SELECT '' \"TABLE_CAT\", schema_name \"TABLE_SCHEM\", NULL \"TABLE_NAME\", "
		                                 "NULL \"TABLE_TYPE\" , NULL \"REMARKS\" FROM information_schema.schemata",
		                      SQL_NTS))) {
			return SQL_ERROR;
		}
		return SQL_SUCCESS;
	}

	if (table_type == string(SQL_ALL_TABLE_TYPES) && NameLength1 == 0 && NameLength2 == 0 && NameLength3 == 0) {
		return SQL_ERROR; // TODO
	}

	// TODO make this a nice template? also going to use this for SQLColumns etc.

	if (!SQL_SUCCEEDED(SQLPrepare(
	        StatementHandle,
	        (SQLCHAR
	             *)"SELECT table_catalog \"TABLE_CAT\", table_schema \"TABLE_SCHEM\", table_name \"TABLE_NAME\", CASE "
	               "WHEN table_type='BASE TABLE' THEN 'TABLE' ELSE table_type END \"TABLE_TYPE\" , '' \"REMARKS\"  "
	               "FROM information_schema.tables WHERE table_schema LIKE ? AND table_name LIKE ? and table_type = ?",
	        SQL_NTS))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLBindParameter(StatementHandle, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0, 0,
	                                    SchemaName, NameLength2, nullptr))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLBindParameter(StatementHandle, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0, 0,
	                                    TableName, NameLength3, nullptr))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLBindParameter(StatementHandle, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0, 0,
	                                    TableType, NameLength4, nullptr))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLExecute(StatementHandle))) {
		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}

SQLRETURN SQLColumns(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1, SQLCHAR *SchemaName,
                     SQLSMALLINT NameLength2, SQLCHAR *TableName, SQLSMALLINT NameLength3, SQLCHAR *ColumnName,
                     SQLSMALLINT NameLength4) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) {
		// TODO
		return SQL_ERROR;
	});
}

SQLRETURN SQLColAttribute(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLUSMALLINT FieldIdentifier,
                          SQLPOINTER CharacterAttributePtr, SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr,
                          SQLLEN *NumericAttributePtr) {
	throw std::runtime_error("SQLColAttribute"); // TODO
}

SQLRETURN SQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (Option != SQL_CLOSE) {
			return SQL_ERROR;
		}
		stmt->res.reset();
		stmt->chunk.reset();
		stmt->stmt.reset();
		stmt->bound_cols.clear();
		stmt->params.clear();
		return SQL_SUCCESS;
	});
}
