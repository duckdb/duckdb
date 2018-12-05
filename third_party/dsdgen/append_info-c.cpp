#include "append_info.h"
#include "append_info.hpp"
#include "config.h"
#include "porting.h"
#include "nulls.h"
#include "date.h"

#include "common/exception.hpp"
#include "storage/data_table.hpp"
#include <cstring>

using namespace tpcds;

append_info *append_info_get(void *info_list, int table_id) {
	auto arr = (tpcds_append_information *)info_list;
	return (append_info *)&arr[table_id];
}

void append_row_start(append_info *info) {
	auto append_info = (tpcds_append_information *)info;
	append_info->col = 0;

	auto &chunk = append_info->chunk;
	auto &table = append_info->table;
	if (chunk.column_count == 0) {
		// initalize the chunk
		auto types = table->GetTypes();
		chunk.Initialize(types);
	} else if (chunk.size() >= STANDARD_VECTOR_SIZE) {
		// flush the chunk
		chunk.Verify();
		table->storage->Append(*table, *append_info->context, chunk);
		// have to reset the chunk
		chunk.Reset();
		append_info->row = 0;
	}
	for (size_t i = 0; i < chunk.column_count; i++) {
		chunk.data[i].count++;
	}
}

void append_row_end(append_info *info) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->col != append_info->chunk.column_count) {
		throw duckdb::Exception("Not all columns were appended to");
	}
	append_info->row++;
}

void append_varchar(append_info *info, const char *value) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->col > append_info->chunk.column_count - 1) {
		throw duckdb::Exception("Out-of-bounds column update");
	}
	if (!nullCheck(append_info->col)) {
		append_info->chunk.data[append_info->col].SetStringValue(append_info->row, value);
	} else {
		append_info->chunk.data[append_info->col].SetNull(append_info->row, true);
	}
	append_info->col++;
}

// TODO: use direct array manipulation for speed, but not now
static void append_value(append_info *info, duckdb::Value v) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->col > append_info->chunk.column_count - 1) {
		throw duckdb::Exception("Out-of-bounds column update");
	}
	if (!nullCheck(append_info->col)) {
		append_info->chunk.data[append_info->col].SetValue(append_info->row, v);
	} else {
		append_info->chunk.data[append_info->col].SetNull(append_info->row, true);
	}
	append_info->col++;
}

void append_key(append_info *info, int64_t value) {
	append_value(info, duckdb::Value::BIGINT(value));
}

void append_integer(append_info *info, int32_t value) {
	append_value(info, duckdb::Value::INTEGER(value));
}

void append_boolean(append_info *info, int32_t value) {
	append_value(info, duckdb::Value::BOOLEAN((int8_t)value));
}

// value is a Julian date
// FIXME: direct int conversion, offsets should be constant
void append_date(append_info *info, int64_t value) {
	date_t dTemp;
	jtodt(&dTemp, (int)value);
	auto ddate = duckdb::Date::FromDate(dTemp.year, dTemp.month, dTemp.day);
	append_value(info, duckdb::Value::DATE(ddate));
}

void append_decimal(append_info *info, decimal_t *val) {
	double dTemp = val->number;
	for (int i = 0; i < val->precision; i++) {
		dTemp /= 10.0;
	}
	append_value(info, duckdb::Value(dTemp));
}
