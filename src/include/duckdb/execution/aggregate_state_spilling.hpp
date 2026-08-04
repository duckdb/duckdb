//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/aggregate_state_spilling.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/function/aggregate_state_layout.hpp"

namespace duckdb {

class ClientContext;

//! Converts aggregate states between their native form (fixed-size rows whose variable size
//! payloads live on an arena allocator) and an exported form (typed columns in fully spillable
//! TupleDataCollections), through the aggregate state export layouts. Native states are used
//! during normal aggregation; the exported form is only used when the aggregation goes external,
//! so that the arenas can be freed and the states can be offloaded with the rest of the data.
class AggregateStateSpilling {
public:
	//! Whether every aggregate in the layout can be exported and imported
	static bool CanSpill(const TupleDataLayout &layout);
	//! The types of the exported form: the layout's group columns (including the hash),
	//! followed by one column per aggregate state
	static vector<LogicalType> ExportedTypes(const TupleDataLayout &layout);
	//! Rewrite native rows into exported rows, appending them to `exported`
	static void ExportStates(ClientContext &context, const TupleDataLayout &layout, TupleDataCollection &source,
	                         PartitionedTupleData &exported, ArenaAllocator &allocator);
	//! Rewrite exported rows back into native rows, allocating variable size state data from `allocator`
	static unique_ptr<TupleDataCollection> ImportStates(ClientContext &context, shared_ptr<TupleDataLayout> layout,
	                                                    TupleDataCollection &exported, ArenaAllocator &allocator);

private:
	//! The state layouts of the layout's aggregates, in aggregate order
	static vector<AggregateStateLayout> StateLayouts(const TupleDataLayout &layout);
};

} // namespace duckdb
