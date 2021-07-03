#include "duckdb_odbc.hpp"
#include "api_info.hpp"

using duckdb::ApiInfo;
using duckdb::idx_t;

/*** ODBC API Functions ********************************/
SQLRETURN SQLGetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr) {
	if (function_id == SQL_API_ODBC3_ALL_FUNCTIONS) {
		return ApiInfo::GetFunctions30(connection_handle, function_id, supported_ptr);
	} else {
		return ApiInfo::GetFunctions(connection_handle, function_id, supported_ptr);
	}
}

SQLRETURN SQLGetTypeInfo(SQLHSTMT statement_handle, SQLSMALLINT data_type) {
	return ApiInfo::CheckDataType(data_type);
}

/*** ApiInfo private attributes ********************************/

// fill all supported functions in this array
const std::vector<SQLUSMALLINT> ApiInfo::ALL_SUPPORTED_FUNCTIONS = {
    SQL_API_SQLALLOCHANDLE,    SQL_API_SQLFREEHANDLE,   SQL_API_SQLGETCONNECTATTR, SQL_API_SQLSETENVATTR,
    SQL_API_SQLSETCONNECTATTR, SQL_API_SQLSETSTMTATTR,  SQL_API_SQLDRIVERCONNECT,  SQL_API_SQLCONNECT,
    SQL_API_SQLGETINFO,        SQL_API_SQLENDTRAN,      SQL_API_SQLDISCONNECT,     SQL_API_SQLTABLES,
    SQL_API_SQLCOLUMNS,        SQL_API_SQLPREPARE,      SQL_API_SQLEXECDIRECT,     SQL_API_SQLFREESTMT,
    SQL_API_SQLDESCRIBEPARAM,  SQL_API_SQLDESCRIBECOL,  SQL_API_SQLCOLATTRIBUTES,  SQL_API_SQLFETCHSCROLL,
    SQL_API_SQLROWCOUNT,       SQL_API_SQLGETDIAGFIELD, SQL_API_SQLGETDIAGREC,     SQL_API_SQLGETFUNCTIONS,
    SQL_API_SQLBINDPARAMETER,  SQL_API_SQLGETDATA,      SQL_API_SQLFETCH,          SQL_API_SQLEXECUTE,
    SQL_API_SQLNUMRESULTCOLS,  SQL_API_SQLGETTYPEINFO};

const std::vector<SQLUSMALLINT> ApiInfo::ODBC3_SUPPORTED_FUNCTIONS = {
    SQL_API_SQLALLOCHANDLE,    SQL_API_SQLFREEHANDLE,   SQL_API_SQLGETCONNECTATTR, SQL_API_SQLSETENVATTR,
    SQL_API_SQLSETCONNECTATTR, SQL_API_SQLSETSTMTATTR,  SQL_API_SQLDRIVERCONNECT,  SQL_API_SQLCONNECT,
    SQL_API_SQLGETINFO,        SQL_API_SQLENDTRAN,      SQL_API_SQLDISCONNECT,     SQL_API_SQLTABLES,
    SQL_API_SQLCOLUMNS,        SQL_API_SQLPREPARE,      SQL_API_SQLEXECDIRECT,     SQL_API_SQLFREESTMT,
    SQL_API_SQLDESCRIBEPARAM,  SQL_API_SQLDESCRIBECOL,  SQL_API_SQLCOLATTRIBUTES,  SQL_API_SQLFETCHSCROLL,
    SQL_API_SQLROWCOUNT,       SQL_API_SQLGETDIAGFIELD, SQL_API_SQLGETDIAGREC,     SQL_API_SQLGETFUNCTIONS,
    SQL_API_SQLBINDPARAMETER,  SQL_API_SQLGETDATA,      SQL_API_SQLFETCH,          SQL_API_SQLEXECUTE,
    SQL_API_SQLNUMRESULTCOLS,  SQL_API_SQLGETTYPEINFO};

const std::vector<SQLSMALLINT> ApiInfo::ODBC_SUPPORTED_SQL_TYPES = {SQL_CHAR,
                                                                    SQL_TINYINT,
                                                                    SQL_SMALLINT,
                                                                    SQL_INTEGER,
                                                                    SQL_BIGINT,
                                                                    SQL_DATE,
                                                                    SQL_TYPE_DATE,
                                                                    SQL_TIME,
                                                                    SQL_TYPE_TIME,
                                                                    SQL_TIMESTAMP,
                                                                    SQL_TYPE_TIMESTAMP,
                                                                    SQL_DECIMAL,
                                                                    SQL_NUMERIC,
                                                                    SQL_FLOAT,
                                                                    SQL_DOUBLE,
                                                                    SQL_VARCHAR,
                                                                    SQL_INTERVAL_MONTH,
                                                                    SQL_INTERVAL_YEAR,
                                                                    SQL_INTERVAL_YEAR_TO_MONTH,
                                                                    SQL_INTERVAL_DAY,
                                                                    SQL_INTERVAL_HOUR,
                                                                    SQL_INTERVAL_MINUTE,
                                                                    SQL_INTERVAL_SECOND,
                                                                    SQL_INTERVAL_DAY_TO_HOUR,
                                                                    SQL_INTERVAL_DAY_TO_MINUTE,
                                                                    SQL_INTERVAL_DAY_TO_SECOND,
                                                                    SQL_INTERVAL_HOUR_TO_MINUTE,
                                                                    SQL_INTERVAL_HOUR_TO_SECOND,
                                                                    SQL_INTERVAL_MINUTE_TO_SECOND};

// Map DuckDB LogicalTypes to ODBC SQL Types
const std::map<duckdb::LogicalTypeId, SQLSMALLINT, ApiInfo::LogicalTypeCmp> ApiInfo::MAP_SQL_TYPES = {
    {LogicalTypeId::BOOLEAN, SQL_CHAR},        {LogicalTypeId::TINYINT, SQL_TINYINT},
    {LogicalTypeId::UTINYINT, SQL_TINYINT},    {LogicalTypeId::SMALLINT, SQL_SMALLINT},
    {LogicalTypeId::USMALLINT, SQL_SMALLINT},  {LogicalTypeId::INTEGER, SQL_INTEGER},
    {LogicalTypeId::UINTEGER, SQL_INTEGER},    {LogicalTypeId::BIGINT, SQL_BIGINT},
    {LogicalTypeId::UBIGINT, SQL_BIGINT},      {LogicalTypeId::FLOAT, SQL_FLOAT},
    {LogicalTypeId::DOUBLE, SQL_DOUBLE},       {LogicalTypeId::DATE, SQL_DATE},
    {LogicalTypeId::TIMESTAMP, SQL_TIMESTAMP}, {LogicalTypeId::TIME, SQL_TIME},
    {LogicalTypeId::VARCHAR, SQL_VARCHAR},     {LogicalTypeId::INTERVAL, SQL_TIME}};

void ApiInfo::SetFunctionSupported(SQLUSMALLINT *flags, int function_id) {
	flags[function_id >> 4] |= (1 << (function_id & 0xF));
}

/*** ApiInfo public functions ***********************************/
SQLRETURN ApiInfo::GetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr) {
	if (function_id == SQL_API_ALL_FUNCTIONS) {
		memset(supported_ptr, SQL_FALSE, sizeof(SQLSMALLINT) * NUM_FUNC_SUPPORTED);
		idx_t func_idx;
		for (idx_t i = 0; i < ALL_SUPPORTED_FUNCTIONS.size(); ++i) {
			func_idx = ALL_SUPPORTED_FUNCTIONS[i];
			supported_ptr[func_idx] = SQL_TRUE;
		}

		return SQL_SUCCESS;
	}

	switch (function_id) {
	// handles
	case SQL_API_SQLALLOCHANDLE:
	case SQL_API_SQLFREEHANDLE:
	// attributes
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
	case SQL_API_SQLEXECUTE:
	case SQL_API_SQLTABLES:
	case SQL_API_SQLCOLUMNS:
	case SQL_API_SQLPREPARE:
	case SQL_API_SQLEXECDIRECT:
	case SQL_API_SQLFREESTMT:
	case SQL_API_SQLDESCRIBEPARAM:
	case SQL_API_SQLDESCRIBECOL:
	case SQL_API_SQLCOLATTRIBUTES:
	case SQL_API_SQLNUMRESULTCOLS:
	// results
	case SQL_API_SQLGETDATA:
	case SQL_API_SQLFETCH:
	case SQL_API_SQLFETCHSCROLL:
	case SQL_API_SQLROWCOUNT:
	case SQL_API_SQLBINDPARAMETER:
	// diagnostics
	case SQL_API_SQLGETDIAGFIELD:
	case SQL_API_SQLGETDIAGREC:
	// api info
	case SQL_API_SQLGETFUNCTIONS:
	case SQL_API_SQLGETTYPEINFO:
		*supported_ptr = SQL_TRUE;
		break;
	default:
		*supported_ptr = SQL_FALSE;
		break;
	}

	return SQL_SUCCESS;
}

SQLRETURN ApiInfo::GetFunctions30(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr) {
	if (function_id != SQL_API_ODBC3_ALL_FUNCTIONS) {
		return SQL_ERROR;
	}
	memset(supported_ptr, 0, sizeof(UWORD) * SQL_API_ODBC3_ALL_FUNCTIONS_SIZE);
	for (idx_t i = 0; i < ODBC3_SUPPORTED_FUNCTIONS.size(); ++i) {
		SetFunctionSupported(supported_ptr, ODBC3_SUPPORTED_FUNCTIONS[i]);
	}
	return SQL_SUCCESS;
}

SQLSMALLINT ApiInfo::FindRelatedSQLType(duckdb::LogicalTypeId type_id) {
	auto it = MAP_SQL_TYPES.find(type_id);
	if (it != MAP_SQL_TYPES.end()) {
		return it->second;
	}
	return SQL_UNKNOWN_TYPE;
}

SQLRETURN ApiInfo::CheckDataType(SQLSMALLINT data_type) {
	for (idx_t i = 0; i < ODBC_SUPPORTED_SQL_TYPES.size(); ++i) {
		if (ODBC_SUPPORTED_SQL_TYPES[i] == data_type) {
			return SQL_SUCCESS;
		}
	}
	return SQL_ERROR;
}