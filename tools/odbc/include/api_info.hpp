#pragma once

#include "duckdb.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <map>
#include <vector>

#define NUM_FUNC_SUPPORTED 4000

namespace duckdb {

struct ApiInfo {

private:
	// fill all supported functions in this array
	static const std::vector<SQLUSMALLINT> all_supported_functions;

	// fill ODBC3 supported functions in this array
	static const std::vector<SQLUSMALLINT> odbc3_supported_functions;

	static const std::vector<SQLSMALLINT> odbc_supported_sql_types;

	struct LogicalTypeCmp {
		bool operator()(const LogicalTypeId a, const LogicalTypeId b) const {
			return a < b;
		}
	};
	// Map DuckDB LogicalTypes to ODBC SQL Types
	static const std::map<duckdb::LogicalTypeId, SQLSMALLINT, LogicalTypeCmp> map_sql_types;

	static void SetFunctionSupported(SQLUSMALLINT *flags, int function_id);

public:
	static SQLRETURN GetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr);

	static SQLRETURN GetFunctions30(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr);

	static SQLSMALLINT FindRelatedSQLType(duckdb::LogicalTypeId type_id);

	static SQLRETURN CheckDataType(SQLSMALLINT data_type);

}; // end ApiInfo struct

} // namespace duckdb