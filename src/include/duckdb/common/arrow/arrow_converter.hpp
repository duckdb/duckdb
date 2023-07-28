//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_converter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/arrow/nanoarrow/nanoarrow.hpp"
#include "duckdb/common/arrow/arrow_options.hpp"

namespace duckdb {

using duckdb_nanoarrow::ArrowSchema;

struct ArrowConverter {
	DUCKDB_API static void ToArrowSchema(ArrowSchema *out_schema, const vector<LogicalType> &types,
	                                     const vector<string> &names, const ArrowOptions &options);
	DUCKDB_API static void ToArrowArray(DataChunk &input, ArrowArray *out_array, ArrowOptions options);
};

} // namespace duckdb
