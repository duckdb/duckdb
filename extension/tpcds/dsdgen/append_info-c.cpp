#include "append_info-c.hpp"

#include "append_info.h"
#include "config.h"
#include "date.h"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/storage/data_table.hpp"
#include "nulls.h"
#include "porting.h"

#include <cstring>
#include <memory>
#include "duckdb/common/unique_ptr.hpp"

using namespace tpcds;

append_info *append_info_get(void *info_list, int table_id) {
	auto &append_vector = *((duckdb::vector<duckdb::unique_ptr<tpcds_append_information>> *)info_list);
	return (append_info *)append_vector[table_id].get();
}

bool tpcds_append_information::IsNull(int nColumn) {
	return nullCheck(nColumn);
}

void append_row_start(append_info info) {
	auto append_info = (tpcds_append_information *)info;
	append_info->appender.BeginRow();
}

void append_row_end(append_info info) {
	auto append_info = (tpcds_append_information *)info;
	append_info->appender.EndRow();
}

void append_varchar(append_info info, const char *value, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn) || !value || *value == '\0') {
		append_info->appender.Append(nullptr);
	} else {
		append_info->appender.Append<duckdb::string_t>(duckdb::string_t(value));
	}
}

void append_key(append_info info, int64_t value, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn) || value < 0) {
		append_info->appender.Append(nullptr);
	} else {
		append_info->appender.Append<int64_t>(value);
	}
}

void append_integer_decimal(append_info info, int32_t val, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn)) {
		append_info->appender.Append(nullptr);
	} else {
		append_info->appender.Append<int32_t>(val * 100);
	}
}

void append_integer(append_info info, int32_t value, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn)) {
		append_info->appender.Append(nullptr);
	} else {
		append_info->appender.Append<int32_t>(value);
	}
}

void append_boolean(append_info info, int32_t value, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn)) {
		append_info->appender.Append(nullptr);
	} else {
		append_info->appender.Append<bool>(value != 0);
	}
}

// value is a Julian date
// FIXME: direct int conversion, offsets should be constant
void append_date(append_info info, int64_t value, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn) || value < 0) {
		append_info->appender.Append(nullptr);
	} else {
		date_t dTemp;
		jtodt(&dTemp, (int)value);
		auto ddate = duckdb::Date::FromDate(dTemp.year, dTemp.month, dTemp.day);
		append_info->appender.Append<duckdb::date_t>(ddate);
	}
}

void append_decimal(append_info info, decimal_t *val, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn)) {
		append_info->appender.Append(nullptr);
		return;
	}
	auto &appender = append_info->appender;
	auto &type = appender.GetActiveTypes()[appender.CurrentColumn()];
	D_ASSERT(type.id() == duckdb::LogicalTypeId::DECIMAL);
	D_ASSERT(duckdb::DecimalType::GetScale(type) == val->precision);
	appender.Append<int64_t>(val->number);
}
