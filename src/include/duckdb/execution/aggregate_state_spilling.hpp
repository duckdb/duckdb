//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/aggregate_state_spilling.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/function/aggregate_state_layout.hpp"

#include <functional>

namespace duckdb {

class ClientContext;

//! The immutable spill metadata of an aggregate layout: its state export layouts and the types
//! of the spillable columnar representation (the group columns and hash, then one column per state)
struct AggregateStateSpillPlan {
	vector<AggregateStateLayout> state_layouts;
	vector<LogicalType> exported_types;
};

//! Converts native aggregate states to and from a spillable columnar representation.
class AggregateStateSpilling {
public:
	//! Returns a spill plan if the layout contains arena-backed states that can be serialized
	//! safely, or nullptr if the layout's states cannot or need not be spilled
	static unique_ptr<AggregateStateSpillPlan> TryCreateSpillPlan(const TupleDataLayout &layout);
	//! Serializes native rows into hash-partitioned column collections
	static void ExportStates(ClientContext &context, const TupleDataLayout &layout, const AggregateStateSpillPlan &plan,
	                         TupleDataCollection &source, vector<unique_ptr<ColumnDataCollection>> &exported,
	                         idx_t exported_radix_bits, ArenaAllocator &allocator);
	//! Imports and combines one chunk of exported states at a time
	static void ImportStates(ClientContext &context, const shared_ptr<TupleDataLayout> &layout,
	                         const AggregateStateSpillPlan &plan, ColumnDataCollection &exported,
	                         ArenaAllocator &allocator, const std::function<void(TupleDataCollection &)> &combine);
};

} // namespace duckdb
