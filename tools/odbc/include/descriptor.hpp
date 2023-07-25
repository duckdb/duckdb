#ifndef DESCRIPTOR_HPP
#define DESCRIPTOR_HPP

// needs to be first because BOOL
#include "duckdb.hpp"

#ifdef _WIN32
#include <Windows.h>
#endif

#include "sqlext.h"
#include <sqltypes.h>

namespace duckdb {

struct DescRecord {
public:
	DescRecord() {};
	DescRecord(const DescRecord &other);
	SQLRETURN SetValueType(SQLSMALLINT value_type);
	SQLRETURN SetSqlDescType(SQLSMALLINT type);
	SQLRETURN SetSqlDataType(SQLSMALLINT type);
	void SetDescUnsignedField(const duckdb::LogicalType &type);

public:
	SQLINTEGER sql_desc_auto_unique_value;
	std::string sql_desc_base_column_name;
	std::string sql_desc_base_table_name;
	SQLINTEGER sql_desc_case_sensitive;
	std::string sql_desc_catalog_name;
	SQLSMALLINT sql_desc_concise_type;
	SQLPOINTER sql_desc_data_ptr;
	SQLSMALLINT sql_desc_datetime_interval_code;
	SQLINTEGER sql_desc_datetime_interval_precision;
	SQLINTEGER sql_desc_display_size;
	SQLSMALLINT sql_desc_fixed_prec_scale;
	SQLLEN *sql_desc_indicator_ptr;
	std::string sql_desc_label;
	SQLULEN sql_desc_length;
	std::string sql_desc_literal_prefix;
	std::string sql_desc_literal_suffix;
	std::string sql_desc_local_type_name;
	std::string sql_desc_name;
	SQLSMALLINT sql_desc_nullable = SQL_NULLABLE_UNKNOWN;
	SQLINTEGER sql_desc_num_prec_radix;
	SQLLEN sql_desc_octet_length;
	SQLLEN *sql_desc_octet_length_ptr;
	SQLSMALLINT sql_desc_parameter_type;
	SQLSMALLINT sql_desc_precision;
	SQLSMALLINT sql_desc_rowver;
	SQLSMALLINT sql_desc_scale;
	std::string sql_desc_schema_name;
	SQLSMALLINT sql_desc_searchable;
	std::string sql_desc_table_name;
	SQLSMALLINT sql_desc_type;
	std::string sql_desc_type_name;
	SQLSMALLINT sql_desc_unnamed;
	SQLSMALLINT sql_desc_unsigned;
	SQLSMALLINT sql_desc_updatable;
};

struct DescHeader {
public:
	DescHeader();
	DescHeader(const DescHeader &other);
	void Reset();

public:
	SQLSMALLINT sql_desc_alloc_type;
	// default value is 1 for array size, this should be used by odbc_fetch
	SQLULEN sql_desc_array_size = 1;
	SQLUSMALLINT *sql_desc_array_status_ptr;
	SQLLEN *sql_desc_bind_offset_ptr = nullptr;
	SQLINTEGER sql_desc_bind_type = SQL_BIND_TYPE_DEFAULT;
	SQLSMALLINT sql_desc_count;
	SQLULEN *sql_desc_rows_processed_ptr;
};
} // namespace duckdb

#endif
