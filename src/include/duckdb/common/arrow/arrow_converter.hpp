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
	DUCKDB_API static void ToArrowSchema(ArrowSchema *out_schema, const vector<LogicalType> &types,
	                                     const vector<string> &names, const string &config_timezone,
	                                     bool export_as_large = true);
	DUCKDB_API static void ToArrowArray(DataChunk &input, ArrowArray *out_array, bool export_as_large = true);
};

} // namespace duckdb
