#include "duckdb_odbc.hpp"
#include <iostream>

//! ODBC stub functions not implemented yet,
//! when implementing the function must be moved to the proper source file
//! Using std::cout instead of throw execptions because of MVSC's warning C4297

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT statement_handle) {
	std::cout << "***** SQLCloseCursor" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLCopyDesc(SQLHDESC source_desc_handle, SQLHDESC target_desc_handle) {
	std::cout << "***** SQLCopyDesc" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLDataSources(SQLHENV environment_handle, SQLUSMALLINT direction, SQLCHAR *server_name,
                                 SQLSMALLINT buffer_length1, SQLSMALLINT *name_length1_ptr, SQLCHAR *description,
                                 SQLSMALLINT buffer_length2, SQLSMALLINT *name_length2_ptr) {
	std::cout << "***** SQLDataSources" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLDrivers(SQLHENV environment_handle, SQLUSMALLINT direction, SQLCHAR *driver_description,
                             SQLSMALLINT buffer_length1, SQLSMALLINT *description_length_ptr,
                             SQLCHAR *driver_attributes, SQLSMALLINT buffer_length2,
                             SQLSMALLINT *attributes_length_ptr) {
	std::cout << "***** SQLDrivers" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT statement_handle, SQLCHAR *cursor_name, SQLSMALLINT buffer_length,
                                   SQLSMALLINT *name_length_ptr) {
	std::cout << "***** SQLGetCursorName" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetDescField(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLSMALLINT field_identifier,
                                  SQLPOINTER value_ptr, SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	std::cout << "***** SQLGetDescField" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV environment_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                                SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	std::cout << "***** SQLGetEnvAttr" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLNativeSql(SQLHDBC connection_handle, SQLCHAR *in_statement_text, SQLINTEGER text_length1,
                               SQLCHAR *out_statement_text, SQLINTEGER buffer_length, SQLINTEGER *text_length2_ptr) {
	std::cout << "***** SQLNativeSql" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT statement_handle, SQLCHAR *cursor_name, SQLSMALLINT name_length) {
	std::cout << "***** SQLSetCursorName" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetDescField(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLSMALLINT field_identifier,
                                  SQLPOINTER value_ptr, SQLINTEGER buffer_length) {
	std::cout << "***** SQLSetDescField" << std::endl;
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

SQLRETURN SQL_API SQLGetDescRec(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLCHAR *name,
                                SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr, SQLSMALLINT *type_ptr,
                                SQLSMALLINT *sub_type_ptr, SQLLEN *length_ptr, SQLSMALLINT *precision_ptr,
                                SQLSMALLINT *scale_ptr, SQLSMALLINT *nullable_ptr) {
	std::cout << "***** SQLGetDescRec" << std::endl;
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

SQLRETURN SQL_API SQLSetDescRec(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLSMALLINT type,
                                SQLSMALLINT sub_type, SQLLEN length, SQLSMALLINT precision, SQLSMALLINT scale,
                                SQLPOINTER data_ptr, SQLLEN *string_length_ptr, SQLLEN *indicator_ptr) {
	std::cout << "***** SQLSetDescRec" << std::endl;
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