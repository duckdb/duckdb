#include "duckdb_odbc.hpp"
#include <iostream>

//! ODBC stub functions not implemented yet,
//! when implementing the function must be moved to the proper source file
//! Using std::cout instead of throw execptions because of MVSC's warning C4297

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT statement_handle) {
	std::cout << "***** SQLCloseCursor" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLCopyDesc(SQLHDESC SourceDescHandle, SQLHDESC TargetDescHandle) {
	std::cout << "***** SQLCopyDesc" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLDataSources(SQLHENV EnvironmentHandle, SQLUSMALLINT Direction, SQLCHAR *ServerName,
                                 SQLSMALLINT BufferLength1, SQLSMALLINT *NameLength1Ptr, SQLCHAR *Description,
                                 SQLSMALLINT BufferLength2, SQLSMALLINT *NameLength2Ptr) {
	std::cout << "***** SQLDataSources" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLDrivers(SQLHENV EnvironmentHandle, SQLUSMALLINT Direction, SQLCHAR *DriverDescription,
                             SQLSMALLINT BufferLength1, SQLSMALLINT *DescriptionLengthPtr, SQLCHAR *DriverAttributes,
                             SQLSMALLINT BufferLength2, SQLSMALLINT *AttributesLengthPtr) {
	std::cout << "***** SQLDrivers" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT StatementHandle, SQLCHAR *CursorName, SQLSMALLINT BufferLength,
                                   SQLSMALLINT *NameLengthPtr) {
	std::cout << "***** SQLGetCursorName" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
                                  SQLPOINTER ValuePtr, SQLINTEGER BufferLength, SQLINTEGER *StringLengthPtr) {
	std::cout << "***** SQLGetDescField" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                                SQLINTEGER BufferLength, SQLINTEGER *StringLengthPtr) {
	std::cout << "***** SQLGetEnvAttr" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLNativeSql(SQLHDBC ConnectionHandle, SQLCHAR *InStatementText, SQLINTEGER TextLength1,
                               SQLCHAR *OutStatementText, SQLINTEGER BufferLength, SQLINTEGER *TextLength2Ptr) {
	std::cout << "***** SQLNativeSql" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT StatementHandle, SQLCHAR *CursorName, SQLSMALLINT NameLength) {
	std::cout << "***** SQLSetCursorName" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
                                  SQLPOINTER ValuePtr, SQLINTEGER BufferLength) {
	std::cout << "***** SQLSetDescField" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT StatementHandle, SQLSMALLINT IdentifierType, SQLCHAR *CatalogName,
                                    SQLSMALLINT NameLength1, SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                    SQLCHAR *TableName, SQLSMALLINT NameLength3, SQLSMALLINT Scope,
                                    SQLSMALLINT Nullable) {
	std::cout << "***** SQLSpecialColumns" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLStatistics(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                SQLCHAR *SchemaName, SQLSMALLINT NameLength2, SQLCHAR *TableName,
                                SQLSMALLINT NameLength3, SQLUSMALLINT Unique, SQLUSMALLINT Reserved) {
	std::cout << "***** SQLStatistics" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLBrowseConnect(SQLHDBC ConnectionHandle, SQLCHAR *InConnectionString, SQLSMALLINT StringLength1,
                                   SQLCHAR *OutConnectionString, SQLSMALLINT BufferLength,
                                   SQLSMALLINT *StringLength2Ptr) {
	std::cout << "***** SQLBrowseConnect" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLBulkOperations(SQLHSTMT StatementHandle, SQLUSMALLINT Operation) {
	std::cout << "***** SQLBulkOperations" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLColumnPrivileges(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                      SQLCHAR *SchemaName, SQLSMALLINT NameLength2, SQLCHAR *TableName,
                                      SQLSMALLINT NameLength3, SQLCHAR *ColumnName, SQLSMALLINT NameLength4) {
	std::cout << "***** SQLColumnPrivileges" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT StatementHandle, SQLCHAR *PKCatalogName, SQLSMALLINT NameLength1,
                                 SQLCHAR *PKSchemaName, SQLSMALLINT NameLength2, SQLCHAR *PKTableName,
                                 SQLSMALLINT NameLength3, SQLCHAR *FKCatalogName, SQLSMALLINT NameLength4,
                                 SQLCHAR *FKSchemaName, SQLSMALLINT NameLength5, SQLCHAR *FKTableName,
                                 SQLSMALLINT NameLength6) {
	std::cout << "***** SQLForeignKeys" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber, SQLCHAR *Name,
                                SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr, SQLSMALLINT *TypePtr,
                                SQLSMALLINT *SubTypePtr, SQLLEN *LengthPtr, SQLSMALLINT *PrecisionPtr,
                                SQLSMALLINT *ScalePtr, SQLSMALLINT *NullablePtr) {
	std::cout << "***** SQLGetDescRec" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                 SQLCHAR *SchemaName, SQLSMALLINT NameLength2, SQLCHAR *TableName,
                                 SQLSMALLINT NameLength3) {
	std::cout << "***** SQLPrimaryKeys" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLProcedureColumns(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                      SQLCHAR *SchemaName, SQLSMALLINT NameLength2, SQLCHAR *ProcName,
                                      SQLSMALLINT NameLength3, SQLCHAR *ColumnName, SQLSMALLINT NameLength4) {
	std::cout << "***** SQLProcedureColumns" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLProcedures(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                SQLCHAR *SchemaName, SQLSMALLINT NameLength2, SQLCHAR *ProcName,
                                SQLSMALLINT NameLength3) {
	std::cout << "***** SQLProcedures" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber, SQLSMALLINT Type, SQLSMALLINT SubType,
                                SQLLEN Length, SQLSMALLINT Precision, SQLSMALLINT Scale, SQLPOINTER DataPtr,
                                SQLLEN *StringLengthPtr, SQLLEN *IndicatorPtr) {
	std::cout << "***** SQLSetDescRec" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetPos(SQLHSTMT StatementHandle, SQLSETPOSIROW RowNumber, SQLUSMALLINT Operation,
                            SQLUSMALLINT LockType) {
	std::cout << "***** SQLSetPos" << std::endl;
	return SQL_ERROR;
}

SQLRETURN SQL_API SQLTablePrivileges(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                     SQLCHAR *SchemaName, SQLSMALLINT NameLength2, SQLCHAR *TableName,
                                     SQLSMALLINT NameLength3) {
	std::cout << "***** SQLTablePrivileges" << std::endl;
	return SQL_ERROR;
}