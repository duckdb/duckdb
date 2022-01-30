#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "statement_functions.hpp"

using duckdb::ApiInfo;
using duckdb::idx_t;
using duckdb::TypeInfo;
using std::string;

/*** ODBC API Functions ********************************/
SQLRETURN SQL_API SQLGetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr) {

	if (function_id == SQL_API_ODBC3_ALL_FUNCTIONS) {
		return ApiInfo::GetFunctions30(connection_handle, function_id, supported_ptr);
	} else {
		return ApiInfo::GetFunctions(connection_handle, function_id, supported_ptr);
	}
}

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT statement_handle, SQLSMALLINT data_type) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN {
		string query("SELECT * FROM (VALUES ");

		if (data_type == SQL_ALL_TYPES) {
			auto vec_types = ApiInfo::GetVectorTypesAddr();
			ApiInfo::WriteInfoTypesToQueryString(vec_types, query);
		} else {
			vector<TypeInfo> vec_types;
			ApiInfo::FindDataType(data_type, vec_types);
			ApiInfo::WriteInfoTypesToQueryString(vec_types, query);
		}

		query += ") AS odbc_types (\"TYPE_NAME\", \"DATA_TYPE\", \"COLUMN_SIZE\", \"LITERAL_PREFIX\", "
		         "\"LITERAL_SUFFIX\", \"CREATE_PARAMS\", \"NULLABLE\", "
		         "\"CASE_SENSITIVE\", \"SEARCHABLE\", \"UNSIGNED_ATTRIBUTE\", \"FIXED_PREC_SCALE\", "
		         "\"AUTO_UNIQUE_VALUE\", \"LOCAL_TYPE_NAME\", "
		         "\"MINIMUM_SCALE\", \"MAXIMUM_SCALE\", \"SQL_DATA_TYPE\", \"SQL_DATETIME_SUB\", \"NUM_PREC_RADIX\", "
		         "\"INTERVAL_PRECISION\")";

		return duckdb::ExecDirectStmt(stmt, (SQLCHAR *)query.c_str(), query.size());
	});
}

/*** ApiInfo private attributes ********************************/

// fill all supported functions in this array
const std::unordered_set<SQLUSMALLINT> ApiInfo::BASE_SUPPORTED_FUNCTIONS = {
    SQL_API_SQLALLOCHANDLE,    SQL_API_SQLFREEHANDLE,   SQL_API_SQLGETCONNECTATTR, SQL_API_SQLSETENVATTR,
    SQL_API_SQLSETCONNECTATTR, SQL_API_SQLSETSTMTATTR,  SQL_API_SQLDRIVERCONNECT,  SQL_API_SQLCONNECT,
    SQL_API_SQLGETINFO,        SQL_API_SQLENDTRAN,      SQL_API_SQLDISCONNECT,     SQL_API_SQLTABLES,
    SQL_API_SQLCOLUMNS,        SQL_API_SQLPREPARE,      SQL_API_SQLEXECDIRECT,     SQL_API_SQLFREESTMT,
    SQL_API_SQLDESCRIBEPARAM,  SQL_API_SQLDESCRIBECOL,  SQL_API_SQLCOLATTRIBUTES,  SQL_API_SQLFETCHSCROLL,
    SQL_API_SQLROWCOUNT,       SQL_API_SQLGETDIAGFIELD, SQL_API_SQLGETDIAGREC,     SQL_API_SQLGETFUNCTIONS,
    SQL_API_SQLBINDPARAMETER,  SQL_API_SQLGETDATA,      SQL_API_SQLFETCH,          SQL_API_SQLEXECUTE,
    SQL_API_SQLNUMRESULTCOLS,  SQL_API_SQLGETTYPEINFO,  SQL_API_SQLBINDCOL,        SQL_API_SQLCANCEL,
    SQL_API_SQLNUMPARAMS,      SQL_API_SQLPARAMDATA,    SQL_API_SQLPUTDATA,        SQL_API_SQLMORERESULTS,
    SQL_API_SQLGETSTMTATTR};

const std::unordered_set<SQLUSMALLINT> ApiInfo::ODBC3_EXTRA_SUPPORTED_FUNCTIONS = {
    SQL_API_SQLCLOSECURSOR,   SQL_API_SQLCOPYDESC,       SQL_API_SQLDATASOURCES,      SQL_API_SQLDRIVERS,
    SQL_API_SQLGETCURSORNAME, SQL_API_SQLGETDESCFIELD,   SQL_API_SQLGETENVATTR,       SQL_API_SQLNATIVESQL,
    SQL_API_SQLSETCURSORNAME, SQL_API_SQLSETDESCFIELD,   SQL_API_SQLSPECIALCOLUMNS,   SQL_API_SQLSTATISTICS,
    SQL_API_SQLBROWSECONNECT, SQL_API_SQLBULKOPERATIONS, SQL_API_SQLCOLUMNPRIVILEGES, SQL_API_SQLFOREIGNKEYS,
    SQL_API_SQLGETDESCREC,    SQL_API_SQLPRIMARYKEYS,    SQL_API_SQLPROCEDURECOLUMNS, SQL_API_SQLPROCEDURES,
    SQL_API_SQLSETDESCREC,    SQL_API_SQLSETPOS,         SQL_API_SQLTABLEPRIVILEGES};

const std::vector<duckdb::TypeInfo> ApiInfo::ODBC_SUPPORTED_SQL_TYPES = {
    {"CHAR", SQL_CHAR, 1, "''''", "''''", "'length'", SQL_NULLABLE, SQL_TRUE, SQL_SEARCHABLE, -1, SQL_FALSE, SQL_FALSE,
     "NULL", -1, -1, SQL_CHAR, -1, -1, -1},
    {"BOOLEAN", SQL_BIT, 1, "NULL", "NULL", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, SQL_TRUE, SQL_TRUE,
     SQL_FALSE, "'boolean'", -1, -1, SQL_BIT, -1, -1, -1},
    {"TINYINT", SQL_TINYINT, 3, "NULL", "NULL", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, SQL_FALSE, SQL_FALSE,
     SQL_FALSE, "NULL", 0, 0, SQL_TINYINT, -1, 2, -1},
    {"SMALLINT", SQL_SMALLINT, 5, "NULL", "NULL", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, SQL_FALSE, SQL_FALSE,
     SQL_FALSE, "NULL", 0, 0, SQL_SMALLINT, -1, 2, -1},
    {"INTEGER", SQL_INTEGER, 10, "NULL", "NULL", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, SQL_FALSE, SQL_FALSE,
     SQL_FALSE, "NULL", 0, 0, SQL_INTEGER, -1, 2, -1},
    {"BIGINT", SQL_BIGINT, 19, "NULL", "NULL", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, SQL_FALSE, SQL_FALSE,
     SQL_FALSE, "NULL", 0, 0, SQL_BIGINT, -1, 2, -1},
    {"DATE", SQL_TYPE_DATE, 10, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1, SQL_FALSE, SQL_FALSE,
     "NULL", -1, -1, SQL_DATETIME, SQL_CODE_DATE, -1, -1},
    {"TIME", SQL_TYPE_TIME, 8, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1, SQL_FALSE, SQL_FALSE,
     "NULL", 0, 0, SQL_DATETIME, SQL_CODE_TIME, -1, -1},
    {"TIMESTAMP", SQL_TYPE_TIMESTAMP, 26, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1, SQL_FALSE,
     SQL_FALSE, "NULL", 0, 0, SQL_DATETIME, SQL_CODE_TIMESTAMP, -1, -1},
    {"DECIMAL", SQL_DECIMAL, 38, "'", "'", "'precision,scale'", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1, SQL_FALSE,
     SQL_FALSE, "NULL", 0, 38, SQL_DECIMAL, -1, 10, -1},
    {"NUMERIC", SQL_NUMERIC, 38, "'", "'", "'precision,scale'", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1, SQL_FALSE,
     SQL_FALSE, "NULL", 0, 38, SQL_NUMERIC, -1, 10, -1},
    {"FLOAT", SQL_FLOAT, 24, "NULL", "NULL", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, SQL_FALSE, SQL_FALSE,
     SQL_FALSE, "NULL", 0, 0, SQL_FLOAT, -1, 2, -1},
    {"DOUBLE", SQL_DOUBLE, 53, "NULL", "NULL", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, SQL_FALSE, SQL_FALSE,
     SQL_FALSE, "NULL", 0, 0, SQL_DOUBLE, -1, 2, -1},
    {"'VARCHAR'", SQL_VARCHAR, -1, "''''", "''''", "'length'", SQL_NULLABLE, SQL_TRUE, SQL_SEARCHABLE, -1, SQL_FALSE,
     -1, "NULL", -1, -1, SQL_VARCHAR, -1, -1, -1},
    {"BLOB", SQL_VARBINARY, -1, "'", "'", "length", SQL_NULLABLE, SQL_TRUE, SQL_PRED_BASIC, -1, SQL_FALSE, SQL_FALSE,
     "NULL", -1, -1, SQL_VARBINARY, -1, -1, -1},
    {"INTERVAL YEAR", SQL_INTERVAL_YEAR, 9, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1, SQL_FALSE,
     -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_YEAR, -1, -1},
    {"INTERVAL MONTH", SQL_INTERVAL_MONTH, 10, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1, SQL_FALSE,
     -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_MONTH, -1, -1},
    {"INTERVAL DAY", SQL_INTERVAL_DAY, 5, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1, SQL_FALSE, -1,
     "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_DAY, -1, -1},
    {"INTERVAL HOUR", SQL_INTERVAL_HOUR, 6, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1, SQL_FALSE,
     -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_HOUR, -1, -1},
    {"INTERVAL MINUTE", SQL_INTERVAL_MINUTE, 8, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1,
     SQL_FALSE, -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_MINUTE, -1, -1},
    {"INTERVAL SECONDE", SQL_INTERVAL_SECOND, 10, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1,
     SQL_FALSE, -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_SECOND, -1, -1},
    {"INTERVAL YEAR TO MONTH", SQL_INTERVAL_YEAR_TO_MONTH, 12, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE,
     SQL_PRED_BASIC, -1, SQL_FALSE, -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_YEAR_TO_MONTH, -1, -1},
    {"INTERVAL DAY TO HOUR", SQL_INTERVAL_DAY_TO_HOUR, 8, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE, SQL_PRED_BASIC, -1,
     SQL_FALSE, -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_DAY_TO_HOUR, -1, -1},
    {"INTERVAL DAY TO MINUTE", SQL_INTERVAL_DAY_TO_MINUTE, 11, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE,
     SQL_PRED_BASIC, -1, SQL_FALSE, -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_DAY_TO_MINUTE, -1, -1},
    {"INTERVAL DAY TO SECOND", SQL_INTERVAL_DAY_TO_SECOND, 14, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE,
     SQL_PRED_BASIC, -1, SQL_FALSE, -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_DAY_TO_SECOND, -1, -1},
    {"INTERVAL HOUR TO MINUTE", SQL_INTERVAL_HOUR_TO_MINUTE, 9, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE,
     SQL_PRED_BASIC, -1, SQL_FALSE, -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_HOUR_TO_MINUTE, -1, -1},
    {"INTERVAL HOUR TO SECOND", SQL_INTERVAL_HOUR_TO_SECOND, 12, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE,
     SQL_PRED_BASIC, -1, SQL_FALSE, -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_HOUR_TO_SECOND, -1, -1},
    {"INTERVAL MINUTE TO SECOND", SQL_INTERVAL_MINUTE_TO_SECOND, 12, "'", "'", "NULL", SQL_NULLABLE, SQL_FALSE,
     SQL_PRED_BASIC, -1, SQL_FALSE, -1, "NULL", 0, 0, SQL_INTERVAL, SQL_CODE_MINUTE_TO_SECOND, -1, -1}};

void ApiInfo::FindDataType(SQLSMALLINT data_type, vector<TypeInfo> &vec_types) {
	duckdb::idx_t size_types = ODBC_SUPPORTED_SQL_TYPES.size();
	for (duckdb::idx_t i = 0; i < size_types; ++i) {
		if (ODBC_SUPPORTED_SQL_TYPES[i].data_type == data_type) {
			// get next info type using the same data type
			for (duckdb::idx_t type_idx = i;
			     type_idx < size_types && ODBC_SUPPORTED_SQL_TYPES[type_idx].data_type == data_type; ++type_idx) {
				vec_types.emplace_back(ODBC_SUPPORTED_SQL_TYPES[type_idx]);
			}
			break;
		}
	}
}

void ApiInfo::WriteInfoTypesToQueryString(const vector<TypeInfo> &vec_types, string &query) {
	for (auto info_type : vec_types) {
		query += "(";
		query += "CAST(" + string(info_type.type_name) + " AS VARCHAR),";
		query += "CAST(" + std::to_string(info_type.data_type) + " AS SMALLINT),";
		query += "CAST(" + std::to_string(info_type.column_size) + " AS INTEGER),";
		query += "CAST(" + string(info_type.literal_prefix) + " AS VARCHAR),";
		query += "CAST(" + string(info_type.literal_suffix) + " AS VARCHAR),";
		query += "CAST(" + string(info_type.create_params) + " AS VARCHAR),";
		query += "CAST(" + std::to_string(info_type.nullable) + " AS SMALLINT),";
		query += "CAST(" + std::to_string(info_type.case_sensitive) + " AS SMALLINT),";
		query += "CAST(" + std::to_string(info_type.searchable) + " AS SMALLINT),";
		query += "CAST(" + std::to_string(info_type.unsigned_attribute) + " AS SMALLINT),";
		query += "CAST(" + std::to_string(info_type.fixed_prec_scale) + " AS SMALLINT),";
		query += "CAST(" + std::to_string(info_type.auto_unique_value) + " AS SMALLINT),";
		query += "CAST(" + string(info_type.local_type_name) + " AS VARCHAR),";
		query += "CAST(" + std::to_string(info_type.minimum_scale) + " AS SMALLINT),";
		query += "CAST(" + std::to_string(info_type.maximum_scale) + " AS SMALLINT),";
		query += "CAST(" + std::to_string(info_type.sql_data_type) + " AS SMALLINT),";
		query += "CAST(" + std::to_string(info_type.sql_datetime_sub) + " AS SMALLINT),";
		query += "CAST(" + std::to_string(info_type.num_prec_radix) + " AS integer),";
		query += "CAST(" + std::to_string(info_type.interval_precision) + " AS SMALLINT)";
		query += ")";
	}
}

void ApiInfo::SetFunctionSupported(SQLUSMALLINT *flags, int function_id) {
	flags[function_id >> 4] |= (1 << (function_id & 0xF));
}

/*** ApiInfo public functions ***********************************/
SQLRETURN ApiInfo::GetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr) {
	if (function_id == SQL_API_ALL_FUNCTIONS) {
		memset(supported_ptr, SQL_FALSE, sizeof(SQLSMALLINT) * NUM_FUNC_SUPPORTED);
		idx_t func_idx;
		for (auto it = BASE_SUPPORTED_FUNCTIONS.begin(); it != BASE_SUPPORTED_FUNCTIONS.end(); it++) {
			func_idx = *it;
			supported_ptr[func_idx] = SQL_TRUE;
		}
		return SQL_SUCCESS;
	}

	*supported_ptr = SQL_FALSE;
	if (BASE_SUPPORTED_FUNCTIONS.find(function_id) != BASE_SUPPORTED_FUNCTIONS.end()) {
		*supported_ptr = SQL_TRUE;
	}

	return SQL_SUCCESS;
}

SQLRETURN ApiInfo::GetFunctions30(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr) {
	if (function_id != SQL_API_ODBC3_ALL_FUNCTIONS) {
		return SQL_ERROR;
	}
	memset(supported_ptr, 0, sizeof(UWORD) * SQL_API_ODBC3_ALL_FUNCTIONS_SIZE);
	for (auto it = BASE_SUPPORTED_FUNCTIONS.begin(); it != BASE_SUPPORTED_FUNCTIONS.end(); it++) {
		SetFunctionSupported(supported_ptr, *it);
	}
	for (auto it = ODBC3_EXTRA_SUPPORTED_FUNCTIONS.begin(); it != ODBC3_EXTRA_SUPPORTED_FUNCTIONS.end(); it++) {
		SetFunctionSupported(supported_ptr, *it);
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
	case LogicalTypeId::HUGEINT:
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
		*col_size_ptr = 3;
		return SQL_SUCCESS;
	case SQL_SMALLINT:
		*col_size_ptr = 5;
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

const vector<TypeInfo> &ApiInfo::GetVectorTypesAddr() {
	return ApiInfo::ODBC_SUPPORTED_SQL_TYPES;
}