#include "duckdb_odbc.hpp"
#include "iostream"

template <duckdb::idx_t N>
duckdb::idx_t GetSize(SQLUSMALLINT (&array)[N]) {
	return N;
}

#define NUM_FUNC_SUPPORTED 4000

// fill all supported functions in this array
SQLUSMALLINT all_supported_functions[] = {
    SQL_API_SQLALLOCHANDLE,    SQL_API_SQLFREEHANDLE,   SQL_API_SQLGETCONNECTATTR, SQL_API_SQLSETENVATTR,
    SQL_API_SQLSETCONNECTATTR, SQL_API_SQLSETSTMTATTR,  SQL_API_SQLDRIVERCONNECT,  SQL_API_SQLCONNECT,
    SQL_API_SQLGETINFO,        SQL_API_SQLENDTRAN,      SQL_API_SQLDISCONNECT,     SQL_API_SQLTABLES,
    SQL_API_SQLCOLUMNS,        SQL_API_SQLPREPARE,      SQL_API_SQLEXECDIRECT,     SQL_API_SQLFREESTMT,
    SQL_API_SQLDESCRIBEPARAM,  SQL_API_SQLDESCRIBECOL,  SQL_API_SQLCOLATTRIBUTES,  SQL_API_SQLFETCHSCROLL,
    SQL_API_SQLROWCOUNT,       SQL_API_SQLGETDIAGFIELD, SQL_API_SQLGETDIAGREC,     SQL_API_SQLGETFUNCTIONS};

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/odbc-api-reference?view=sql-server-ver15

SQLRETURN GetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr) {
	if (function_id == SQL_API_ALL_FUNCTIONS) {
		memset(supported_ptr, SQL_FALSE, sizeof(SQLSMALLINT) * NUM_FUNC_SUPPORTED);
		duckdb::idx_t func_idx;
		for (duckdb::idx_t i = 0; i < GetSize(all_supported_functions); ++i) {
			func_idx = all_supported_functions[i];
			supported_ptr[func_idx] = SQL_TRUE;
		}

		return SQL_SUCCESS;
	}

	switch (function_id) {
	// handles
	case SQL_API_SQLALLOCHANDLE:
	case SQL_API_SQLFREEHANDLE:
	// attirbutes
	case SQL_API_SQLGETCONNECTATTR:
	case SQL_API_SQLSETENVATTR:
	case SQL_API_SQLSETCONNECTATTR:
	case SQL_API_SQLSETSTMTATTR:
	// connections
	case SQL_API_SQLDRIVERCONNECT:
	case SQL_API_SQLCONNECT:
	case SQL_API_SQLGETINFO:
	case SQL_API_SQLENDTRAN:
	case SQL_API_SQLDISCONNECT:
	// statements
	case SQL_API_SQLTABLES:
	case SQL_API_SQLCOLUMNS:
	case SQL_API_SQLPREPARE:
	case SQL_API_SQLEXECDIRECT:
	case SQL_API_SQLFREESTMT:
	case SQL_API_SQLDESCRIBEPARAM:
	case SQL_API_SQLDESCRIBECOL:
	case SQL_API_SQLCOLATTRIBUTES:
	// results
	case SQL_API_SQLFETCHSCROLL:
	case SQL_API_SQLROWCOUNT:
	// diagnostics
	case SQL_API_SQLGETDIAGFIELD:
	case SQL_API_SQLGETDIAGREC:
	// api info
	case SQL_API_SQLGETFUNCTIONS:
		*supported_ptr = SQL_TRUE;
		break;
	default:
		*supported_ptr = SQL_FALSE;
		break;
	}

	return SQL_SUCCESS;
}

static void SetFunctionSupported(SQLUSMALLINT *flags, int function_id) {
	flags[function_id >> 4] |= (1 << (function_id & 0xF));
}

// fill ODBC3 supported functions in this array
SQLUSMALLINT odbc3_supported_functions[] = {
    SQL_API_SQLALLOCHANDLE,    SQL_API_SQLFREEHANDLE,   SQL_API_SQLGETCONNECTATTR, SQL_API_SQLSETENVATTR,
    SQL_API_SQLSETCONNECTATTR, SQL_API_SQLSETSTMTATTR,  SQL_API_SQLDRIVERCONNECT,  SQL_API_SQLCONNECT,
    SQL_API_SQLGETINFO,        SQL_API_SQLENDTRAN,      SQL_API_SQLDISCONNECT,     SQL_API_SQLTABLES,
    SQL_API_SQLCOLUMNS,        SQL_API_SQLPREPARE,      SQL_API_SQLEXECDIRECT,     SQL_API_SQLFREESTMT,
    SQL_API_SQLDESCRIBEPARAM,  SQL_API_SQLDESCRIBECOL,  SQL_API_SQLCOLATTRIBUTES,  SQL_API_SQLFETCHSCROLL,
    SQL_API_SQLROWCOUNT,       SQL_API_SQLGETDIAGFIELD, SQL_API_SQLGETDIAGREC,     SQL_API_SQLGETFUNCTIONS};

SQLRETURN GetFunctions30(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr) {
	if (function_id != SQL_API_ODBC3_ALL_FUNCTIONS) {
		return SQL_ERROR;
	}
	memset(supported_ptr, 0, sizeof(UWORD) * SQL_API_ODBC3_ALL_FUNCTIONS_SIZE);
	for (duckdb::idx_t i = 0; i < GetSize(odbc3_supported_functions); ++i) {
		SetFunctionSupported(supported_ptr, odbc3_supported_functions[i]);
	}
	return SQL_SUCCESS;
}

SQLRETURN SQLGetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr) {
	if (function_id == SQL_API_ODBC3_ALL_FUNCTIONS) {
		return GetFunctions30(connection_handle, function_id, supported_ptr);
	} else {
		return GetFunctions(connection_handle, function_id, supported_ptr);
	}
}
