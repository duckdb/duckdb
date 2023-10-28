#pragma once

#include "duckdb/common/arrow/appender/append_data.hpp"

namespace duckdb {

struct ArrowListData {
public:
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity);
	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size);
	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result);

public:
	static void AppendOffsets(ArrowAppendData &append_data, UnifiedVectorFormat &format, idx_t from, idx_t to,
	                          vector<sel_t> &child_sel);
};

} // namespace duckdb
