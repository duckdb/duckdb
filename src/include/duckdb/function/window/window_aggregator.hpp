//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_aggregator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"

namespace duckdb {

class WindowCollection;
class WindowCursor;
struct WindowSharedExpressions;

class WindowAggregatorState {
public:
	WindowAggregatorState();
	virtual ~WindowAggregatorState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

	//! Allocator for aggregates
	ArenaAllocator allocator;
};

class WindowAggregator {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	WindowAggregator(const BoundWindowExpression &wexpr, const WindowExcludeMode exclude_mode_p);
	WindowAggregator(const BoundWindowExpression &wexpr, const WindowExcludeMode exclude_mode_p,
	                 WindowSharedExpressions &shared);
	virtual ~WindowAggregator();

	//	Threading states
	virtual unique_ptr<WindowAggregatorState> GetGlobalState(ClientContext &context, idx_t group_count,
	                                                         const ValidityMask &partition_mask) const;
	virtual unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const = 0;

	//	Build
	virtual void Sink(WindowAggregatorState &gstate, WindowAggregatorState &lstate, DataChunk &sink_chunk,
	                  DataChunk &coll_chunk, idx_t input_idx, optional_ptr<SelectionVector> filter_sel, idx_t filtered);
	virtual void Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate, CollectionPtr collection,
	                      const FrameStats &stats);

	//	Probe
	virtual void Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate, const DataChunk &bounds,
	                      Vector &result, idx_t count, idx_t row_idx) const = 0;

	//! The window function
	const BoundWindowExpression &wexpr;
	//! A description of the aggregator
	const AggregateObject aggr;
	//! The argument types for the function
	vector<LogicalType> arg_types;
	//! The result type of the window function
	const LogicalType result_type;
	//! The size of a single aggregate state
	const idx_t state_size;
	//! The window exclusion clause
	const WindowExcludeMode exclude_mode;
	//! Partition collection column indicies
	vector<column_t> child_idx;
};

class WindowAggregatorGlobalState : public WindowAggregatorState {
public:
	WindowAggregatorGlobalState(ClientContext &context, const WindowAggregator &aggregator_p, idx_t group_count)
	    : aggregator(aggregator_p), aggr(aggregator.wexpr), locals(0), finalized(0) {

		if (aggr.filter) {
			// 	Start with all invalid and set the ones that pass
			filter_mask.Initialize(group_count, false);
		} else {
			filter_mask.InitializeEmpty(group_count);
		}
	}

	//! The aggregator data
	const WindowAggregator &aggregator;

	//! The aggregate function
	const AggregateObject aggr;

	//! The filtered rows in inputs.
	ValidityArray filter_mask;

	//! Lock for single threading
	mutable mutex lock;

	//! Count of local tasks
	mutable std::atomic<idx_t> locals;

	//! Number of finalised states
	std::atomic<idx_t> finalized;
};

class WindowAggregatorLocalState : public WindowAggregatorState {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	WindowAggregatorLocalState() {
	}

	void Sink(WindowAggregatorGlobalState &gastate, DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t row_idx);
	virtual void Finalize(WindowAggregatorGlobalState &gastate, CollectionPtr collection);

	//! The state used for reading the collection
	unique_ptr<WindowCursor> cursor;
};

} // namespace duckdb
