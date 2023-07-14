#ifndef ODBC_UTILS_HPP
#define ODBC_UTILS_HPP

// needs to be first because BOOL
#include "duckdb.hpp"

#ifdef _WIN32
#include <Windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <string>

#include "duckdb/common/vector.hpp"

namespace duckdb {
struct OdbcUtils {
public:
	static std::string ReadString(const SQLPOINTER ptr, const SQLSMALLINT len);
	// static void WriteString(const std::string &s, SQLCHAR *out_buf, SQLSMALLINT buf_len, SQLSMALLINT *out_len);
	template <typename INT_TYPE>
	static void WriteString(const string &s, SQLCHAR *out_buf, SQLSMALLINT buf_len, INT_TYPE *out_len = nullptr) {
		INT_TYPE written_chars = 0;
		if (out_buf) {
			written_chars = (INT_TYPE)snprintf((char *)out_buf, buf_len, "%s", s.c_str());
		}
		if (out_len) {
			*out_len = written_chars;
		}
	}
	// template specialization for int to pass a null pointer
	static void WriteString(const string &s, SQLCHAR *out_buf, SQLSMALLINT buf_len) {
		WriteString<int>(s, out_buf, buf_len, nullptr);
	}

	template <typename FIELD_TYPE>
	SQLRETURN IsValidPtrForSpecificedField(SQLPOINTER value_ptr, FIELD_TYPE target_field,
	                                       const vector<FIELD_TYPE> vec_field_ids) {
		for (auto field_id : vec_field_ids) {
			// target field doens't accept null_ptr
			if (field_id == target_field && value_ptr == nullptr) {
				return SQL_ERROR;
			}
		}
		return SQL_SUCCESS;
	}

	static bool IsCharType(SQLSMALLINT type);

	static SQLRETURN SetStringValueLength(const std::string &val_str, SQLLEN *str_len_or_ind_ptr);
	static SQLRETURN SetStringAndLength(vector<std::string> &error_messages, const std::string &val_str,
	                                    SQLPOINTER target_value_ptr, SQLSMALLINT buffer_length,
	                                    SQLSMALLINT *str_len_or_ind_ptr);

	static std::string GetStringAsIdentifier(const std::string &str);
	static std::string ParseStringFilter(const std::string &filter_name, const std::string &filter_value,
	                                     SQLUINTEGER sql_attr_metadata_id, const std::string &coalesce_str = "");

	static std::string GetQueryDuckdbTables(const std::string &schema_filter, const std::string &table_filter,
	                                        const std::string &table_type_filter);
	static std::string GetQueryDuckdbColumns(const std::string &catalog_filter, const std::string &schema_filter,
	                                         const std::string &table_filter, const std::string &column_filter);

	static void SetValueFromConnStr(const string &conn_str, const char *key, string &value);
	static void SetValueFromConnStr(SQLCHAR *conn_c_str, const char *key, string &value);

	static SQLUINTEGER SQLPointerToSQLUInteger(SQLPOINTER value);
};
} // namespace duckdb
#endif
