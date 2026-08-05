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

//! Converts aggregate states between their native form (fixed-size rows whose variable size
//! payloads live on an arena allocator) and an exported form (typed columns in fully spillable
//! TupleDataCollections), through the aggregate state export layouts. Native states are used
//! during normal aggregation; the exported form is only used when the aggregation goes external,
//! so that the arenas can be freed and the states can be offloaded with the rest of the data.
class AggregateStateSpilling {
public:
	//! Whether the layout's aggregate states should be exported under memory pressure.
	//! This requires three properties, and is currently all-or-nothing per layout:
	//! - every aggregate can describe its state (it has a state export layout)
	//! - no state owns resources beyond the arena (no state destructor; per-aggregate lifecycle
	//!   such as destroy-after-export or destructive export is not modeled yet)
	//! - at least one state can reference the arena (pointer-free layouts never benefit)
	//! Whether the arena currently holds memory is a runtime decision made by the caller.
	static bool CanSpill(const TupleDataLayout &layout);
	//! The state export layouts of the layout's aggregates, in aggregate order.
	//! Fixed per bound layout, so callers should compute them once and reuse them.
	static vector<AggregateStateLayout> StateLayouts(const TupleDataLayout &layout);
	//! The types of the exported form: the layout's group columns (including the hash),
	//! followed by one column per aggregate state
	static vector<LogicalType> ExportedTypes(const TupleDataLayout &layout);
	//! Rewrite native rows into exported rows, routing them to the exported partitions by the
	//! given radix bits. Only the partitions covered by the source's hash prefix are appended to.
	static void ExportStates(ClientContext &context, const TupleDataLayout &layout,
	                         const vector<AggregateStateLayout> &state_layouts, TupleDataCollection &source,
	                         vector<unique_ptr<ColumnDataCollection>> &exported, idx_t exported_radix_bits,
	                         const vector<LogicalType> &exported_types, ArenaAllocator &allocator);
	//! Rewrite exported rows back into native rows, allocating variable size state data from
	//! `allocator`. The native rows are produced one chunk-sized collection at a time, so that the
	//! whole partition is never materialized alongside its exported copy.
	static void ImportStates(ClientContext &context, shared_ptr<TupleDataLayout> layout,
	                         const vector<AggregateStateLayout> &state_layouts, ColumnDataCollection &exported,
	                         ArenaAllocator &allocator, const std::function<void(TupleDataCollection &)> &combine);
};

} // namespace duckdb
