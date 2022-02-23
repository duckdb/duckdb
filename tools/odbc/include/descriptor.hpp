#ifndef DESCRIPTOR_HPP
#define DESCRIPTOR_HPP

// needs to be first because BOOL
#include "duckdb.hpp"

#ifdef _WIN32
#include <Windows.h>
#endif

#include <sqltypes.h>

namespace duckdb {

struct DescRecord {
public:
	DescRecord() {};
	SQLRETURN SetValueType(SQLSMALLINT value_type);

public:
	SQLINTEGER sql_desc_auto_unique_value;
	SQLCHAR *sql_desc_base_column_name;
	SQLCHAR *sql_desc_base_table_name;
	SQLINTEGER sql_desc_case_sensitive;
	SQLCHAR *sql_desc_catalog_name;
	SQLSMALLINT sql_desc_concise_type;
	SQLPOINTER sql_desc_data_ptr;
	SQLSMALLINT sql_desc_datetime_interval_code;
	SQLINTEGER sql_desc_datetime_interval_precision;
	SQLLEN sql_desc_display_size;
	SQLSMALLINT sql_desc_fixed_prec_scale;
	SQLLEN *sql_desc_indicator_ptr;
	SQLCHAR *sql_desc_label;
	SQLULEN sql_desc_length;
	SQLCHAR *sql_desc_literal_prefix;
	SQLCHAR *sql_desc_literal_suffix;
	SQLCHAR *sql_desc_local_type_name;
	SQLCHAR *sql_desc_name;
	SQLSMALLINT sql_desc_nullable;
	SQLINTEGER sql_desc_num_prec_radix;
	SQLLEN sql_desc_octet_length;
	SQLLEN *sql_desc_octet_length_ptr;
	SQLSMALLINT sql_desc_parameter_type;
	SQLSMALLINT sql_desc_precision;
	SQLSMALLINT sql_desc_rowver;
	SQLSMALLINT sql_desc_scale;
	SQLCHAR *sql_desc_schema_name;
	SQLSMALLINT sql_desc_searchable;
	SQLCHAR *sql_desc_table_name;
	SQLSMALLINT sql_desc_type;
	SQLCHAR *sql_desc_type_name;
	SQLSMALLINT sql_desc_unnamed;
	SQLSMALLINT sql_desc_unsigned;
	SQLSMALLINT sql_desc_updatable;
};

struct DescHeader {
public:
	DescHeader();
	void Reset();

public:
	SQLSMALLINT sql_desc_alloc_type;
	SQLULEN sql_desc_array_size;
	SQLUSMALLINT *sql_desc_array_status_ptr;
	SQLLEN *sql_desc_bind_offset_ptr = nullptr;
	SQLINTEGER sql_desc_bind_type;
	SQLSMALLINT sql_desc_count;
	SQLULEN *sql_desc_rows_processed_ptr;
};
} // namespace duckdb

#endif