#include "duckdb_odbc.hpp"

#include <iostream>

//! ODBC stub functions not implemented yet,
//! when implementing the function must be moved to the proper source file
//! Using std::cout instead of throw execptions because of MVSC's warning C4297

SQLRETURN SQL_API SQLNativeSql(SQLHDBC connection_handle, SQLCHAR *in_statement_text, SQLINTEGER text_length1,
                               SQLCHAR *out_statement_text, SQLINTEGER buffer_length, SQLINTEGER *text_length2_ptr) {
	std::cout << "***** SQLNativeSql" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT statement_handle, SQLSMALLINT identifier_type, SQLCHAR *catalog_name,
                                    SQLSMALLINT name_length1, SQLCHAR *schema_name, SQLSMALLINT name_length2,
                                    SQLCHAR *table_name, SQLSMALLINT name_length3, SQLSMALLINT scope,
                                    SQLSMALLINT nullable) {
	std::cout << "***** SQLSpecialColumns" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLStatistics(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                SQLSMALLINT name_length3, SQLUSMALLINT unique, SQLUSMALLINT reserved) {
	std::cout << "***** SQLStatistics" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLBrowseConnect(SQLHDBC connection_handle, SQLCHAR *in_connection_string, SQLSMALLINT string_length1,
                                   SQLCHAR *out_connection_string, SQLSMALLINT buffer_length,
                                   SQLSMALLINT *string_length2_ptr) {
	std::cout << "***** SQLBrowseConnect" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLBulkOperations(SQLHSTMT statement_handle, SQLUSMALLINT operation) {
	std::cout << "***** SQLBulkOperations" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLColumnPrivileges(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                      SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                      SQLSMALLINT name_length3, SQLCHAR *column_name, SQLSMALLINT name_length4) {
	std::cout << "***** SQLColumnPrivileges" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT statement_handle, SQLCHAR *pk_catalog_name, SQLSMALLINT name_length1,
                                 SQLCHAR *pk_schema_name, SQLSMALLINT name_length2, SQLCHAR *pk_table_name,
                                 SQLSMALLINT name_length3, SQLCHAR *fk_catalog_name, SQLSMALLINT name_length4,
                                 SQLCHAR *fk_schema_name, SQLSMALLINT name_length5, SQLCHAR *fk_table_name,
                                 SQLSMALLINT name_length6) {
	std::cout << "***** SQLForeignKeys" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                 SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                 SQLSMALLINT name_length3) {
	std::cout << "***** SQLPrimaryKeys" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLProcedureColumns(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                      SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *proc_name,
                                      SQLSMALLINT name_length3, SQLCHAR *column_name, SQLSMALLINT name_length4) {
	std::cout << "***** SQLProcedureColumns" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLProcedures(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *proc_name,
                                SQLSMALLINT name_length3) {
	std::cout << "***** SQLProcedures" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetPos(SQLHSTMT statement_handle, SQLSETPOSIROW row_number, SQLUSMALLINT operation,
                            SQLUSMALLINT lock_type) {
	std::cout << "***** SQLSetPos" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLTablePrivileges(SQLHSTMT statement_handle, SQLCHAR *catalog_name, SQLSMALLINT name_length1,
                                     SQLCHAR *schema_name, SQLSMALLINT name_length2, SQLCHAR *table_name,
                                     SQLSMALLINT name_length3) {
	std::cout << "***** SQLTablePrivileges" << std::endl;
	return SQL_ERROR;
}
