//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_converter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/arrow/arrow.hpp"

struct ArrowSchema;

namespace duckdb {

struct ArrowConverter {
	DUCKDB_API static void ToArrowSchema(ArrowSchema *out_schema, vector<LogicalType> &types, vector<string> &names,
	                                     string &config_timezone);
	DUCKDB_API static void ToArrowArray(DataChunk &input, ArrowArray *out_array);
};

} // namespace duckdb
