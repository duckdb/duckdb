#include "duckdb_odbc.hpp"

SQLRETURN SQLGetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                            SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {

	return duckdb::WithConnection(connection_handle, [&](duckdb::OdbcHandleDbc *dbc) {
		if (!value_ptr) {
			return SQL_ERROR;
		}
		switch (attribute) {
		case SQL_ATTR_AUTOCOMMIT:
			*(SQLUINTEGER *)value_ptr = dbc->autocommit;
			return SQL_SUCCESS;
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQLSetConnectAttr(SQLHDBC connection_handle, SQLINTEGER attribute, SQLPOINTER value_ptr,
                            SQLINTEGER string_length) {
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
			default:
				return SQL_ERROR;
			}
			break;
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQLGetInfo(SQLHDBC connection_handle, SQLUSMALLINT info_type, SQLPOINTER info_value_ptr,
                     SQLSMALLINT buffer_length, SQLSMALLINT *string_length_ptr) {

	// TODO more from fun list
	// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver15

	switch (info_type) {
	case SQL_DRIVER_NAME:
	case SQL_DBMS_NAME: {
		std::string dbname = "DuckDB";
		duckdb::OdbcUtils::WriteString(dbname, (SQLCHAR *)info_value_ptr, buffer_length, string_length_ptr);
		return SQL_SUCCESS;
	}
	case SQL_DBMS_VER: {
		SQLHDBC stmt;

		if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, connection_handle, &stmt))) {
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

		SQLRETURN ret;
		if (string_length_ptr) {
			SQLLEN len_ptr;
			ret = SQLGetData(stmt, 1, SQL_C_CHAR, info_value_ptr, buffer_length, &len_ptr);
			*string_length_ptr = len_ptr;
		} else {
			ret = SQLGetData(stmt, 1, SQL_C_CHAR, info_value_ptr, buffer_length, NULL);
		}
		if (!SQL_SUCCEEDED(ret)) {
			SQLFreeHandle(SQL_HANDLE_STMT, stmt);
			return SQL_ERROR;
		}

		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
		return SQL_SUCCESS;
	}
	case SQL_NON_NULLABLE_COLUMNS:
		// TODO assert buffer length >= sizeof(SQLUSMALLINT)
		duckdb::Store<SQLUSMALLINT>(SQL_NNC_NON_NULL, (duckdb::data_ptr_t)info_value_ptr);
		return SQL_SUCCESS;

	case SQL_ODBC_INTERFACE_CONFORMANCE:
		// TODO assert buffer length >= sizeof(SQLUINTEGER)

		duckdb::Store<SQLUINTEGER>(SQL_OIC_CORE, (duckdb::data_ptr_t)info_value_ptr);

		return SQL_SUCCESS;

	case SQL_CREATE_TABLE:
		// TODO assert buffer length >= sizeof(SQLUINTEGER)

		duckdb::Store<SQLUINTEGER>(SQL_CT_CREATE_TABLE, (duckdb::data_ptr_t)info_value_ptr);

		return SQL_SUCCESS;

	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLEndTran(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT completion_type) {
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
			dbc->conn->Rollback();
			return SQL_SUCCESS;
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQLDisconnect(SQLHDBC connection_handle) {
	return duckdb::WithConnection(connection_handle, [&](duckdb::OdbcHandleDbc *dbc) {
		dbc->conn.reset();
		return SQL_SUCCESS;
	});
}
