//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/window_segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/common/enums/window_aggregation_mode.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

namespace duckdb {

struct WindowSharedExpressions;
class WindowCollection;

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

// Used for validation
class WindowNaiveAggregator : public WindowAggregator {
public:
	WindowNaiveAggregator(const BoundWindowExpression &wexpr, const WindowExcludeMode exclude_mode,
	                      WindowSharedExpressions &shared);
	~WindowNaiveAggregator() override;

	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate, const DataChunk &bounds,
	              Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowConstantAggregator : public WindowAggregator {
public:
	WindowConstantAggregator(const BoundWindowExpression &wexpr, WindowExcludeMode exclude_mode_p,
	                         WindowSharedExpressions &shared);
	~WindowConstantAggregator() override {
	}

	unique_ptr<WindowAggregatorState> GetGlobalState(ClientContext &context, idx_t group_count,
	                                                 const ValidityMask &partition_mask) const override;
	void Sink(WindowAggregatorState &gstate, WindowAggregatorState &lstate, DataChunk &sink_chunk,
	          DataChunk &coll_chunk, idx_t input_idx, optional_ptr<SelectionVector> filter_sel,
	          idx_t filtered) override;
	void Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate, CollectionPtr collection,
	              const FrameStats &stats) override;

	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate, const DataChunk &bounds,
	              Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowCustomAggregator : public WindowAggregator {
public:
	WindowCustomAggregator(const BoundWindowExpression &wexpr, const WindowExcludeMode exclude_mode,
	                       WindowSharedExpressions &shared);
	~WindowCustomAggregator() override;

	unique_ptr<WindowAggregatorState> GetGlobalState(ClientContext &context, idx_t group_count,
	                                                 const ValidityMask &partition_mask) const override;
	void Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate, CollectionPtr collection,
	              const FrameStats &stats) override;

	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate, const DataChunk &bounds,
	              Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowSegmentTree : public WindowAggregator {

public:
	WindowSegmentTree(const BoundWindowExpression &wexpr, WindowAggregationMode mode_p,
	                  const WindowExcludeMode exclude_mode, WindowSharedExpressions &shared);

	unique_ptr<WindowAggregatorState> GetGlobalState(ClientContext &context, idx_t group_count,
	                                                 const ValidityMask &partition_mask) const override;
	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate, CollectionPtr collection,
	              const FrameStats &stats) override;

	void Evaluate(const WindowAggregatorState &gstate, WindowAggregatorState &lstate, const DataChunk &bounds,
	              Vector &result, idx_t count, idx_t row_idx) const override;

public:
	//! Use the combine API, if available
	inline bool UseCombineAPI() const {
		return mode < WindowAggregationMode::SEPARATE;
	}

	//! Use the combine API, if available
	WindowAggregationMode mode;
};

class WindowDistinctAggregator : public WindowAggregator {
public:
	WindowDistinctAggregator(const BoundWindowExpression &wexpr, const WindowExcludeMode exclude_mode_p,
	                         WindowSharedExpressions &shared, ClientContext &context);

	//	Build
	unique_ptr<WindowAggregatorState> GetGlobalState(ClientContext &context, idx_t group_count,
	                                                 const ValidityMask &partition_mask) const override;
	void Sink(WindowAggregatorState &gsink, WindowAggregatorState &lstate, DataChunk &sink_chunk, DataChunk &coll_chunk,
	          idx_t input_idx, optional_ptr<SelectionVector> filter_sel, idx_t filtered) override;
	void Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate, CollectionPtr collection,
	              const FrameStats &stats) override;

	//	Evaluate
	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate, const DataChunk &bounds,
	              Vector &result, idx_t count, idx_t row_idx) const override;

	//! Context for sorting
	ClientContext &context;
};

} // namespace duckdb
