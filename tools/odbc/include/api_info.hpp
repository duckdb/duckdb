#ifndef API_INFO_HPP
#define API_INFO_HPP

#include "duckdb.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <unordered_set>
#include <set>

#define NUM_FUNC_SUPPORTED 4000

using std::vector;

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

	static const vector<TypeInfo> ODBC_SUPPORTED_SQL_TYPES;

	static void SetFunctionSupported(SQLUSMALLINT *flags, int function_id);

public:
	static SQLRETURN GetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr);

	static SQLRETURN GetFunctions30(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr);

	static SQLSMALLINT FindRelatedSQLType(duckdb::LogicalTypeId type_id);

	static void FindDataType(SQLSMALLINT data_type, vector<TypeInfo> &vec_types);

	static SQLLEN PointerSizeOf(SQLSMALLINT sql_type);

	static SQLRETURN GetColumnSize(const duckdb::LogicalType &logical_type, SQLULEN *col_size_ptr);

	static const vector<TypeInfo> &GetVectorTypesAddr();

	static void WriteInfoTypesToQueryString(const vector<TypeInfo> &vec_types, string &query);

}; // end ApiInfo struct

} // namespace duckdb

#endif // API_INFO_HPP