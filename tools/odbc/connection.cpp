#include "duckdb_odbc.hpp"
using namespace duckdb;

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function
SQLRETURN SQLSetConnectAttr(SQLHDBC ConnectionHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                            SQLINTEGER StringLength) {
	return WithConnection(ConnectionHandle, [&](OdbcHandleDbc *dbc) {
		switch (Attribute) {
		case SQL_ATTR_AUTOCOMMIT:
			// assert StringLength == SQL_IS_UINTEGER
			switch ((ptrdiff_t)ValuePtr) {
			case (ptrdiff_t)SQL_AUTOCOMMIT_ON:
				dbc->conn->SetAutoCommit(true);
				return SQL_SUCCESS;
			case (ptrdiff_t)SQL_AUTOCOMMIT_OFF:
				dbc->conn->SetAutoCommit(false);
				return SQL_SUCCESS;
			default:
				return SQL_ERROR;
			}
			break;
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQLGetInfo(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType, SQLPOINTER InfoValuePtr, SQLSMALLINT BufferLength,
                     SQLSMALLINT *StringLengthPtr) {

	// TODO more from fun list
	// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver15

	switch (InfoType) {
	case SQL_DRIVER_NAME:
	case SQL_DBMS_NAME: {
		string dbname = "DuckDB";
		OdbcUtils::WriteString(dbname, (SQLCHAR *)InfoValuePtr, BufferLength, StringLengthPtr);
		return SQL_SUCCESS;
	}
	case SQL_DBMS_VER: {
		SQLHDBC stmt;

		if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, ConnectionHandle, &stmt))) {
			SQLFreeHandle(SQL_HANDLE_STMT, stmt);
			return SQL_ERROR;
		}
		if (!SQL_SUCCEEDED(SQLExecDirect(stmt, (SQLCHAR *)"SELECT library_version FROM pragma_version()", SQL_NTS))) {
			SQLFreeHandle(SQL_HANDLE_STMT, stmt);
			return SQL_ERROR;
		}
		if (!SQL_SUCCEEDED(SQLFetch(stmt))) {
			SQLFreeHandle(SQL_HANDLE_STMT, stmt);
			return SQL_ERROR;
		}
		if (!SQL_SUCCEEDED(SQLGetData(stmt, 1, SQL_C_CHAR, InfoValuePtr, BufferLength, (SQLLEN *)StringLengthPtr))) {
			SQLFreeHandle(SQL_HANDLE_STMT, stmt);
			return SQL_ERROR;
		}
		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
		return SQL_SUCCESS;
	}
	case SQL_NON_NULLABLE_COLUMNS:
		// TODO assert buffer length >= sizeof(SQLUSMALLINT)
		Store<SQLUSMALLINT>(SQL_NNC_NON_NULL, (data_ptr_t)InfoValuePtr);
		return SQL_SUCCESS;

	case SQL_ODBC_INTERFACE_CONFORMANCE:
		// TODO assert buffer length >= sizeof(SQLUINTEGER)

		Store<SQLUINTEGER>(SQL_OIC_CORE, (data_ptr_t)InfoValuePtr);

		return SQL_SUCCESS;

	case SQL_CREATE_TABLE:
		// TODO assert buffer length >= sizeof(SQLUINTEGER)

		Store<SQLUINTEGER>(SQL_CT_CREATE_TABLE, (data_ptr_t)InfoValuePtr);

		return SQL_SUCCESS;

	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLEndTran(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT CompletionType) {
	if (HandleType != SQL_HANDLE_DBC) { // theoretically this can also be done on env but no no no
		return SQL_ERROR;
	}
	return WithConnection(Handle, [&](OdbcHandleDbc *dbc) {
		switch (CompletionType) {
		case SQL_COMMIT:
			dbc->conn->Commit();
		case SQL_ROLLBACK:
			dbc->conn->Rollback();
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQLDisconnect(SQLHDBC ConnectionHandle) {
	return WithConnection(ConnectionHandle, [&](OdbcHandleDbc *dbc) {
		dbc->conn.reset();
		return SQL_SUCCESS;
	});
}
