//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_states.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

struct TupleDataManagementState {
	unordered_map<uint32_t, BufferHandle> row_handles;
	unordered_map<uint32_t, BufferHandle> heap_handles;
};

struct TupleDataAppendState {
	TupleDataManagementState management_state;
	vector<UnifiedVectorFormat> vector_data;
	Vector row_locations = Vector(LogicalType::POINTER);
	Vector heap_locations = Vector(LogicalType::POINTER);
	Vector heap_row_sizes = Vector(LogicalType::UBIGINT);
};

} // namespace duckdb
