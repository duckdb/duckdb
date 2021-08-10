#ifndef API_INFO_HPP
#define API_INFO_HPP

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
	static const std::vector<SQLUSMALLINT> ALL_SUPPORTED_FUNCTIONS;

	// fill ODBC3 supported functions in this array
	static const std::vector<SQLUSMALLINT> ODBC3_SUPPORTED_FUNCTIONS;

	static const std::vector<SQLSMALLINT> ODBC_SUPPORTED_SQL_TYPES;

	static void SetFunctionSupported(SQLUSMALLINT *flags, int function_id);

public:
	static SQLRETURN GetFunctions(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr);

	static SQLRETURN GetFunctions30(SQLHDBC connection_handle, SQLUSMALLINT function_id, SQLUSMALLINT *supported_ptr);

	static SQLSMALLINT FindRelatedSQLType(duckdb::LogicalTypeId type_id);

	static SQLRETURN CheckDataType(SQLSMALLINT data_type);

	static SQLLEN PointerSizeOf(SQLSMALLINT sql_type);

	static SQLRETURN GetColumnSize(duckdb::LogicalType logical_type, SQLLEN *col_size_ptr);

}; // end ApiInfo struct

} // namespace duckdb

#endif // API_INFO_HPP