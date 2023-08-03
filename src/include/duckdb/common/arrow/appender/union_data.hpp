#pragma once

#include "duckdb/common/arrow/appender/append_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Unions
//===--------------------------------------------------------------------===//
/**
 * Based on https://arrow.apache.org/docs/format/Columnar.html#union-layout &
 * https://arrow.apache.org/docs/format/CDataInterface.html
 */
struct ArrowUnionData {
public:
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity);
	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size);
	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result);
};

} // namespace duckdb
