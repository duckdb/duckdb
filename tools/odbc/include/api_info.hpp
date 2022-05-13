#ifndef API_INFO_HPP
#define API_INFO_HPP

#include "duckdb.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <unordered_set>
#include <set>

#define NUM_FUNC_SUPPORTED 4000

namespace duckdb {

struct TypeInfo {
public:
	const char *type_name;
	const int data_type;
	const int column_size;
	const char *literal_prefix;
	const char *literal_suffix;
	const char *create_params;
	const int nullable;
	const int case_sensitive;
	const int searchable;
	const int unsigned_attribute;
	const int fixed_prec_scale;
	const int auto_unique_value;
	const char *local_type_name;
	const int minimum_scale;
	const int maximum_scale;
	const int sql_data_type;
	const int sql_datetime_sub;
	const int num_prec_radix;
	const int interval_precision;
};

struct ApiInfo {
private:
	// fill all supported functions in this array
	static const std::unordered_set<SQLUSMALLINT> BASE_SUPPORTED_FUNCTIONS;

	// fill ODBC3 supported functions in this array
	static const std::unordered_set<SQLUSMALLINT> ODBC3_EXTRA_SUPPORTED_FUNCTIONS;

	// static const std::unordered_set<SQLSMALLINT> ODBC_SUPPORTED_SQL_TYPES;

	static const std::vector<TypeInfo> ODBC_SUPPORTED_SQL_TYPES;

	static void SetFunctionSupported(SQLUSMALLINT *flags, int function_id);

public:
	static SQLRETURN GetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr);

	static SQLRETURN GetFunctions30(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr);

	static SQLSMALLINT FindRelatedSQLType(duckdb::LogicalTypeId type_id);

	static void FindDataType(SQLSMALLINT data_type, std::vector<TypeInfo> &vec_types);

	static SQLLEN PointerSizeOf(SQLSMALLINT sql_type);

	static const std::vector<TypeInfo> &GetVectorTypesAddr();

	static void WriteInfoTypesToQueryString(const std::vector<TypeInfo> &vec_types, string &query);

	static bool IsNumericDescriptorField(SQLSMALLINT field_identifier);

	static bool IsNumericInfoType(SQLUSMALLINT info_type);

	//! https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/display-size?view=sql-server-ver15
	template <typename INT_TYPE>
	SQLRETURN static GetColumnSize(const duckdb::LogicalType &logical_type, INT_TYPE *col_size_ptr) {
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
			// https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/column-size?view=sql-server-ver15
			//*col_size_ptr = SQL_NO_TOTAL; // causes bad alloc
			// we don't know the number of characters, but set because of ADO
			*col_size_ptr = 256;
			return SQL_SUCCESS;
		case SQL_VARBINARY:
			// we don't know the number of characters, but set because of ADO
			*col_size_ptr = 512;
			return SQL_SUCCESS;
		default:
			return SQL_ERROR;
		}
	}

}; // end ApiInfo struct

} // namespace duckdb

#endif // API_INFO_HPP