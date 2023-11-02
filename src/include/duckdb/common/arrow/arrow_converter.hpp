//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_converter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_properties.hpp"

namespace duckdb {

struct ArrowConverter {
	DUCKDB_API static void ToArrowSchema(ArrowSchema *out_schema, const vector<LogicalType> &types,
	                                     const vector<string> &names, const ClientProperties &options);
	DUCKDB_API static void ToArrowArray(DataChunk &input, ArrowArray *out_array, ClientProperties options);

	DUCKDB_API static void ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, ArrowArrayScanState &array_state, idx_t size,
                                const ArrowType &arrow_type, int64_t nested_offset = -1,
                                ValidityMask *parent_mask = nullptr, uint64_t parent_offset = 0);
};

} // namespace duckdb
