#ifndef ODBC_UTIL_HPP
#define ODBC_UTIL_HPP

#include "duckdb.hpp"

#include <sqltypes.h>
#include <string>
#include <vector>

using std::string;
using std::vector;

namespace duckdb {
struct OdbcUtils {
public:
	static string ReadString(const SQLPOINTER ptr, const SQLSMALLINT len);
	static void WriteString(const string &s, SQLCHAR *out_buf, SQLSMALLINT buf_len, SQLSMALLINT *out_len);

	static SQLRETURN SetStringValueLength(const string &val_str, SQLLEN *str_len_or_ind_ptr);
	static SQLRETURN SetStringAndLength(vector<string> &error_messages, const string &val_str,
	                                    SQLPOINTER target_value_ptr, SQLSMALLINT buffer_length,
	                                    SQLSMALLINT *str_len_or_ind_ptr);

	static string GetStringAsIdentifier(const string str);
	static string ParseStringFilter(const string filter_name, const string filter_value,
	                                SQLUINTEGER sql_attr_metadata_id, const string coalesce_str = "");

	static string GetQueryDuckdbTables(const string schema_filter, const string table_filter,
	                                   const string table_type_filter);
	static string GetQueryDuckdbColumns(const string catalog_filter, const string schema_filter,
	                                    const string table_filter, const string column_filter);
};
} // namespace duckdb
#endif