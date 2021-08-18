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
    SQL_API_SQLNUMRESULTCOLS,  SQL_API_SQLGETTYPEINFO,  SQL_API_SQLBINDCOL,        SQL_API_SQLCANCEL,
    SQL_API_SQLNUMPARAMS,      SQL_API_SQLMORERESULTS};

const std::vector<SQLUSMALLINT> ApiInfo::ODBC3_SUPPORTED_FUNCTIONS = {
    SQL_API_SQLALLOCHANDLE,    SQL_API_SQLFREEHANDLE,   SQL_API_SQLGETCONNECTATTR, SQL_API_SQLSETENVATTR,
    SQL_API_SQLSETCONNECTATTR, SQL_API_SQLSETSTMTATTR,  SQL_API_SQLDRIVERCONNECT,  SQL_API_SQLCONNECT,
    SQL_API_SQLGETINFO,        SQL_API_SQLENDTRAN,      SQL_API_SQLDISCONNECT,     SQL_API_SQLTABLES,
    SQL_API_SQLCOLUMNS,        SQL_API_SQLPREPARE,      SQL_API_SQLEXECDIRECT,     SQL_API_SQLFREESTMT,
    SQL_API_SQLDESCRIBEPARAM,  SQL_API_SQLDESCRIBECOL,  SQL_API_SQLCOLATTRIBUTES,  SQL_API_SQLFETCHSCROLL,
    SQL_API_SQLROWCOUNT,       SQL_API_SQLGETDIAGFIELD, SQL_API_SQLGETDIAGREC,     SQL_API_SQLGETFUNCTIONS,
    SQL_API_SQLBINDPARAMETER,  SQL_API_SQLGETDATA,      SQL_API_SQLFETCH,          SQL_API_SQLEXECUTE,
    SQL_API_SQLNUMRESULTCOLS,  SQL_API_SQLGETTYPEINFO,  SQL_API_SQLBINDCOL,        SQL_API_SQLCANCEL,
    SQL_API_SQLNUMPARAMS,      SQL_API_SQLMORERESULTS};

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
                                                                    SQL_BINARY,
                                                                    SQL_VARBINARY,
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
	case SQL_API_SQLBINDCOL:
	case SQL_API_SQLCANCEL:
	case SQL_API_SQLNUMPARAMS:
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
	switch (type_id) {
	case LogicalTypeId::BOOLEAN:
		return SQL_CHAR;
	case LogicalTypeId::TINYINT:
		return SQL_TINYINT;
	case LogicalTypeId::UTINYINT:
		return SQL_TINYINT;
	case LogicalTypeId::SMALLINT:
		return SQL_SMALLINT;
	case LogicalTypeId::USMALLINT:
		return SQL_SMALLINT;
	case LogicalTypeId::INTEGER:
		return SQL_INTEGER;
	case LogicalTypeId::UINTEGER:
		return SQL_INTEGER;
	case LogicalTypeId::BIGINT:
		return SQL_BIGINT;
	case LogicalTypeId::UBIGINT:
		return SQL_BIGINT;
	case LogicalTypeId::FLOAT:
		return SQL_FLOAT;
	case LogicalTypeId::DOUBLE:
		return SQL_DOUBLE;
	case LogicalTypeId::DATE:
		return SQL_DATE;
	case LogicalTypeId::TIMESTAMP:
		return SQL_TIMESTAMP;
	case LogicalTypeId::TIME:
		return SQL_TIME;
	case LogicalTypeId::VARCHAR:
		return SQL_VARCHAR;
	case LogicalTypeId::BLOB:
		return SQL_VARBINARY;
	case LogicalTypeId::INTERVAL:
		return SQL_TIME;
	default:
		return SQL_UNKNOWN_TYPE;
	}
}

SQLRETURN ApiInfo::CheckDataType(SQLSMALLINT data_type) {
	for (idx_t i = 0; i < ODBC_SUPPORTED_SQL_TYPES.size(); ++i) {
		if (ODBC_SUPPORTED_SQL_TYPES[i] == data_type) {
			return SQL_SUCCESS;
		}
	}
	return SQL_ERROR;
}

/**
 * It receives the SQL_C type
 * Returns the size of bytes to increment by a pointer
 * Returns "-1" for variable-length type, e.g., string
 */
SQLLEN ApiInfo::PointerSizeOf(SQLSMALLINT sql_type) {
	switch (sql_type) {
	case SQL_C_SSHORT:
		return sizeof(int16_t);
	case SQL_C_USHORT:
		return sizeof(uint16_t);
	case SQL_C_LONG:
	case SQL_C_SLONG:
		return sizeof(int32_t);
	case SQL_C_ULONG:
		return sizeof(uint32_t);
	case SQL_C_FLOAT:
		return sizeof(float);
	case SQL_C_DOUBLE:
		return sizeof(double);
	case SQL_C_STINYINT:
		return sizeof(int8_t);
	case SQL_C_UTINYINT:
		return sizeof(uint8_t);
	case SQL_C_SBIGINT:
		return sizeof(int64_t);
	case SQL_C_UBIGINT:
		return sizeof(uint64_t);
	case SQL_C_NUMERIC:
	case SQL_C_TYPE_DATE:
	case SQL_C_TYPE_TIME:
	case SQL_C_TYPE_TIMESTAMP:
	case SQL_C_INTERVAL_YEAR:
	case SQL_C_INTERVAL_MONTH:
	case SQL_C_INTERVAL_DAY:
	case SQL_C_INTERVAL_HOUR:
	case SQL_C_INTERVAL_MINUTE:
	case SQL_C_INTERVAL_SECOND:
	case SQL_C_INTERVAL_YEAR_TO_MONTH:
	case SQL_C_INTERVAL_DAY_TO_HOUR:
	case SQL_C_INTERVAL_DAY_TO_MINUTE:
	case SQL_C_INTERVAL_DAY_TO_SECOND:
	case SQL_C_INTERVAL_HOUR_TO_MINUTE:
	case SQL_C_INTERVAL_HOUR_TO_SECOND:
	case SQL_C_INTERVAL_MINUTE_TO_SECOND:
		return sizeof(uint64_t);
	case SQL_C_BIT:
		return sizeof(char);
	case SQL_C_WCHAR:
	case SQL_C_BINARY:
	case SQL_C_CHAR:
	default:
		return -1;
	}
}

//! https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/display-size?view=sql-server-ver15
SQLRETURN ApiInfo::GetColumnSize(const duckdb::LogicalType &logical_type, SQLULEN *col_size_ptr) {
	auto sql_type = FindRelatedSQLType(logical_type.id());
	switch (sql_type) {
	case SQL_DECIMAL:
	case SQL_NUMERIC:
		*col_size_ptr = duckdb::DecimalType::GetWidth(logical_type) + duckdb::DecimalType::GetScale(logical_type);
		return SQL_SUCCESS;
	case SQL_BIT:
		*col_size_ptr = 1;
		return SQL_SUCCESS;
	case SQL_TINYINT:
		*col_size_ptr = 6;
		return SQL_SUCCESS;
	case SQL_INTEGER:
		*col_size_ptr = 11;
		return SQL_SUCCESS;
	case SQL_BIGINT:
		*col_size_ptr = 20;
		return SQL_SUCCESS;
	case SQL_REAL:
		*col_size_ptr = 14;
		return SQL_SUCCESS;
	case SQL_FLOAT:
	case SQL_DOUBLE:
		*col_size_ptr = 24;
		return SQL_SUCCESS;
	case SQL_TYPE_DATE:
		*col_size_ptr = 10;
		return SQL_SUCCESS;
	case SQL_TYPE_TIME:
		*col_size_ptr = 9;
		return SQL_SUCCESS;
	case SQL_TYPE_TIMESTAMP:
		*col_size_ptr = 20;
		return SQL_SUCCESS;
	case SQL_VARCHAR:
	case SQL_VARBINARY:
		// we don't know the number of characters
		*col_size_ptr = 0;
		return SQL_SUCCESS;
	default:
		return SQL_ERROR;
	}
}
