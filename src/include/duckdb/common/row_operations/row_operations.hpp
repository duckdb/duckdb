//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/row_operations/row_operations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

class ArenaAllocator;
struct AggregateObject;
struct AggregateFilterData;
class DataChunk;
class RowLayout;
class TupleDataLayout;
class RowDataCollection;
struct SelectionVector;
class StringHeap;
struct UnifiedVectorFormat;

struct RowOperationsState {
	explicit RowOperationsState(ArenaAllocator &allocator) : allocator(allocator) {
	}

	ArenaAllocator &allocator;
	unique_ptr<Vector> addresses; // Re-usable vector for row_aggregate.cpp
};

// RowOperations contains a set of operations that operate on data using a TupleDataLayout
struct RowOperations {
	//===--------------------------------------------------------------------===//
	// Aggregation Operators
	//===--------------------------------------------------------------------===//
	//! initialize - unaligned addresses
	static void InitializeStates(TupleDataLayout &layout, Vector &addresses, const SelectionVector &sel, idx_t count);
	//! destructor - unaligned addresses, updated
	static void DestroyStates(RowOperationsState &state, TupleDataLayout &layout, Vector &addresses, idx_t count);
	//! update - aligned addresses
	static void UpdateStates(RowOperationsState &state, AggregateObject &aggr, Vector &addresses, DataChunk &payload,
	                         idx_t arg_idx, idx_t count);
	//! filtered update - aligned addresses
	static void UpdateFilteredStates(RowOperationsState &state, AggregateFilterData &filter_data, AggregateObject &aggr,
	                                 Vector &addresses, DataChunk &payload, idx_t arg_idx);
	//! combine - unaligned addresses, updated
	static void CombineStates(RowOperationsState &state, TupleDataLayout &layout, Vector &sources, Vector &targets,
	                          idx_t count);
	//! finalize - unaligned addresses, updated
	static void FinalizeStates(RowOperationsState &state, TupleDataLayout &layout, Vector &addresses, DataChunk &result,
	                           idx_t aggr_idx);
};

} // namespace duckdb
