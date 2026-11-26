#include "append_info-c.hpp"

#include "append_info.h"
#include "config.h"
#include "porting.h"

#include <cstdlib>
#include <cstring>
#include <string>

using namespace tpcds;
using duckdb::cxx::DataChunk;
using duckdb::cxx::varchar_t;

static constexpr idx_t INVALID_ROW = static_cast<idx_t>(-1);

//! Parses the width and scale of "DECIMAL(width, scale)"
static void ParseDecimal(const std::string &type_name, uint32_t &width, uint32_t &scale) {
	auto open = type_name.find('(');
	auto comma = type_name.find(',');
	if (open == std::string::npos || comma == std::string::npos) {
		INTERNAL("unexpected TPC-DS decimal type");
	}
	width = static_cast<uint32_t>(std::strtoul(type_name.c_str() + open + 1, nullptr, 10));
	scale = static_cast<uint32_t>(std::strtoul(type_name.c_str() + comma + 1, nullptr, 10));
}

TPCDSTableLayout::TPCDSTableLayout(duckdb::cxx::Factory &factory, const char *const *type_names,
                                   idx_t column_count) {
	for (idx_t i = 0; i < column_count; i++) {
		std::string type_name(type_names[i]);
		types.push_back(factory.ParseType(type_name));
		decimal_scales.push_back(0);
		if (type_name == "INTEGER") {
			append_types.push_back(TPCDSAppendType::INT32);
		} else if (type_name == "BIGINT") {
			append_types.push_back(TPCDSAppendType::INT64);
		} else if (type_name == "VARCHAR") {
			append_types.push_back(TPCDSAppendType::VARCHAR);
		} else if (type_name == "BOOLEAN") {
			append_types.push_back(TPCDSAppendType::BOOLEAN);
		} else if (type_name == "DATE") {
			append_types.push_back(TPCDSAppendType::DATE);
		} else if (type_name.rfind("DECIMAL", 0) == 0) {
			uint32_t width;
			ParseDecimal(type_name, width, decimal_scales.back());
			if (width <= 4) {
				append_types.push_back(TPCDSAppendType::DECIMAL16);
			} else if (width <= 9) {
				append_types.push_back(TPCDSAppendType::DECIMAL32);
			} else if (width <= 18) {
				append_types.push_back(TPCDSAppendType::DECIMAL64);
			} else {
				append_types.push_back(TPCDSAppendType::DECIMAL128);
			}
		} else {
			INTERNAL("unexpected TPC-DS column type");
		}
	}
}

tpcds_append_information::tpcds_append_information(duckdb::cxx::Factory &factory, const TPCDSTableLayout &layout_p)
    : layout(layout_p), chunk(factory, layout_p.types), buffer(factory, layout_p.types) {
	append_state = std::make_unique<duckdb::cxx::ColumnDataCollection::AppendState>(buffer.CreateAppendState());
	capacity = chunk.GetCapacity();
	PrepareChunk();
}

void tpcds_append_information::PrepareChunk() {
	vectors.clear();
	column_data.clear();
	heaps.clear();
	validity.clear();
	for (idx_t col = 0; col < layout.types.size(); col++) {
		auto vector = chunk.GetVector(col);
		vector.SetSize(capacity);
		column_data.push_back(vector.GetDataMutable());
		if (layout.append_types[col] == TPCDSAppendType::VARCHAR) {
			heaps.push_back(std::make_unique<duckdb::cxx::Arena>(vector.GetHeap()));
		} else {
			heaps.push_back(nullptr);
		}
		validity.push_back(nullptr);
		vectors.push_back(std::move(vector));
	}
}

bool tpcds_append_information::IsNull(int nColumn) {
	auto column_offset = nColumn - table_def.first_column;
	auto bit_mask = int64_t(1) << column_offset;
	return ((*table_def.null_bitmap & bit_mask) != 0);
}

void tpcds_append_information::BeginRow() {
	if (row >= capacity) {
		FlushChunk();
	}
	active_row = row;
	active_col = 0;
}

void tpcds_append_information::EndRow() {
	if (active_col != layout.types.size()) {
		INTERNAL("a TPC-DS row was not completely written");
	}
	row++;
	active_row = INVALID_ROW;
}

void tpcds_append_information::FlushChunk() {
	if (row == 0) {
		return;
	}
	for (auto &vector : vectors) {
		vector.SetSize(row);
	}
	buffer.Append(*append_state, chunk);
	chunk.Clear();
	row = 0;
	active_col = 0;
	PrepareChunk();
}

void tpcds_append_information::ResetBuffer() {
	append_state.reset();
	buffer.Clear();
	append_state = std::make_unique<duckdb::cxx::ColumnDataCollection::AppendState>(buffer.CreateAppendState());
}

idx_t tpcds_append_information::NextColumn() {
	return active_col++;
}

TPCDSAppendType tpcds_append_information::ActiveAppendType() const {
	return layout.append_types[active_col];
}

uint32_t tpcds_append_information::ActiveDecimalScale() const {
	return layout.decimal_scales[active_col];
}

template <class T>
static T *ColumnData(std::vector<void *> &column_data, idx_t col) {
	return static_cast<T *>(column_data[col]);
}

void tpcds_append_information::AppendNull() {
	auto col = NextColumn();
	if (!validity[col]) {
		validity[col] = vectors[col].GetValidityMutable().words;
	}
	validity[col][active_row >> 6] &= ~(uint64_t(1) << (active_row & 63));
}

void tpcds_append_information::AppendVarchar(const char *value) {
	auto col = NextColumn();
	// dsdgen text is ASCII
	ColumnData<varchar_t>(column_data, col)[active_row] = heaps[col]->AddStringUnsafe(value);
}

void tpcds_append_information::AppendKey(int64_t value) {
	auto append_type = ActiveAppendType();
	auto col = NextColumn();
	switch (append_type) {
	case TPCDSAppendType::INT64:
		ColumnData<int64_t>(column_data, col)[active_row] = value;
		break;
	case TPCDSAppendType::INT32:
		ColumnData<int32_t>(column_data, col)[active_row] = static_cast<int32_t>(value);
		break;
	case TPCDSAppendType::VARCHAR:
		ColumnData<varchar_t>(column_data, col)[active_row] = heaps[col]->AddStringUnsafe(std::to_string(value));
		break;
	default:
		INTERNAL("unexpected TPC-DS key column type");
	}
}

void tpcds_append_information::AppendInteger(int32_t value) {
	auto append_type = ActiveAppendType();
	auto col = NextColumn();
	switch (append_type) {
	case TPCDSAppendType::INT64:
		ColumnData<int64_t>(column_data, col)[active_row] = value;
		break;
	case TPCDSAppendType::INT32:
		ColumnData<int32_t>(column_data, col)[active_row] = value;
		break;
	case TPCDSAppendType::VARCHAR:
		ColumnData<varchar_t>(column_data, col)[active_row] = heaps[col]->AddStringUnsafe(std::to_string(value));
		break;
	default:
		INTERNAL("unexpected TPC-DS integer column type");
	}
}

void tpcds_append_information::AppendIntegerDecimal(int32_t value) {
	AppendDecimal(static_cast<int64_t>(value) * 100);
}

void tpcds_append_information::AppendBoolean(int32_t value) {
	if (ActiveAppendType() != TPCDSAppendType::BOOLEAN) {
		INTERNAL("unexpected TPC-DS boolean column type");
	}
	auto col = NextColumn();
	ColumnData<bool>(column_data, col)[active_row] = value != 0;
}

void tpcds_append_information::AppendDate(int32_t days) {
	if (ActiveAppendType() != TPCDSAppendType::DATE) {
		INTERNAL("unexpected TPC-DS date column type");
	}
	auto col = NextColumn();
	ColumnData<duckdb::cxx::date_t>(column_data, col)[active_row] = duckdb::cxx::date_t {days};
}

void tpcds_append_information::AppendDecimal(int64_t value) {
	auto append_type = ActiveAppendType();
	auto col = NextColumn();
	switch (append_type) {
	case TPCDSAppendType::DECIMAL16:
		ColumnData<int16_t>(column_data, col)[active_row] = static_cast<int16_t>(value);
		break;
	case TPCDSAppendType::DECIMAL32:
		ColumnData<int32_t>(column_data, col)[active_row] = static_cast<int32_t>(value);
		break;
	case TPCDSAppendType::DECIMAL64:
		ColumnData<int64_t>(column_data, col)[active_row] = value;
		break;
	case TPCDSAppendType::DECIMAL128:
		ColumnData<duckdb::cxx::int128_t>(column_data, col)[active_row] =
		    duckdb::cxx::int128_t {static_cast<uint64_t>(value), value < 0 ? -1 : 0};
		break;
	default:
		INTERNAL("unexpected TPC-DS decimal column type");
	}
}

append_info *append_info_get(void *info_list, int table_id) {
	auto &append_vector = *((std::vector<std::unique_ptr<tpcds_append_information>> *)info_list);
	return (append_info *)append_vector[table_id].get();
}

void append_row_start(append_info info) {
	auto append_info = (tpcds_append_information *)info;
	append_info->BeginRow();
}

void append_row_end(append_info info) {
	auto append_info = (tpcds_append_information *)info;
	append_info->EndRow();
}

void append_varchar(append_info info, const char *value, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn) || !value || *value == '\0') {
		append_info->AppendNull();
	} else {
		append_info->AppendVarchar(value);
	}
}

void append_key(append_info info, int64_t value, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn) || value < 0) {
		append_info->AppendNull();
	} else {
		append_info->AppendKey(value);
	}
}

void append_integer_decimal(append_info info, int32_t val, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn)) {
		append_info->AppendNull();
	} else {
		append_info->AppendIntegerDecimal(val);
	}
}

void append_integer(append_info info, int32_t value, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn)) {
		append_info->AppendNull();
	} else {
		append_info->AppendInteger(value);
	}
}

void append_boolean(append_info info, int32_t value, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn)) {
		append_info->AppendNull();
	} else {
		append_info->AppendBoolean(value);
	}
}

// value is a Julian date
void append_date(append_info info, int64_t value, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn) || value < 0) {
		append_info->AppendNull();
	} else {
		static constexpr int64_t UNIX_EPOCH_JULIAN = 2440588;
		append_info->AppendDate(static_cast<int32_t>(value - UNIX_EPOCH_JULIAN));
	}
}

void append_decimal(append_info info, decimal_t *val, int nColumn) {
	auto append_info = (tpcds_append_information *)info;
	if (append_info->IsNull(nColumn)) {
		append_info->AppendNull();
		return;
	}
	append_info->AppendDecimal(val->number);
}
