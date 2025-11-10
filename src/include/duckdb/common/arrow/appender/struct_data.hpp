//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/appender/struct_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/appender/append_data.hpp"
#include "duckdb/common/arrow/appender/scalar_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Structs
//===--------------------------------------------------------------------===//
struct ArrowStructData {
public:
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity);
	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size);
	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result);
};

} // namespace duckdb
