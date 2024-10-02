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
	WindowAggregator(AggregateObject aggr, const vector<LogicalType> &arg_types_p, const LogicalType &result_type_p,
	                 const WindowExcludeMode exclude_mode_p);
	virtual ~WindowAggregator();

	//	Threading states
	virtual unique_ptr<WindowAggregatorState> GetGlobalState(idx_t group_count,
	                                                         const ValidityMask &partition_mask) const;
	virtual unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const = 0;

	//	Build
	virtual void Sink(WindowAggregatorState &gstate, WindowAggregatorState &lstate, DataChunk &arg_chunk,
	                  idx_t input_idx, optional_ptr<SelectionVector> filter_sel, idx_t filtered);
	virtual void Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate, const FrameStats &stats);

	//	Probe
	virtual void Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate, const DataChunk &bounds,
	                      Vector &result, idx_t count, idx_t row_idx) const = 0;

	//! A description of the aggregator
	const AggregateObject aggr;
	//! The argument types for the function
	const vector<LogicalType> arg_types;
	//! The result type of the window function
	const LogicalType result_type;
	//! The size of a single aggregate state
	const idx_t state_size;
	//! The window exclusion clause
	const WindowExcludeMode exclude_mode;
};

// Used for validation
class WindowNaiveAggregator : public WindowAggregator {
public:
	WindowNaiveAggregator(AggregateObject aggr, const vector<LogicalType> &arg_types_p,
	                      const LogicalType &result_type_p, const WindowExcludeMode exclude_mode);
	~WindowNaiveAggregator() override;

	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate, const DataChunk &bounds,
	              Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowConstantAggregator : public WindowAggregator {
public:
	WindowConstantAggregator(AggregateObject aggr, const vector<LogicalType> &arg_types_p,
	                         const LogicalType &result_type_p, WindowExcludeMode exclude_mode_p);
	~WindowConstantAggregator() override {
	}

	unique_ptr<WindowAggregatorState> GetGlobalState(idx_t group_count,
	                                                 const ValidityMask &partition_mask) const override;
	void Sink(WindowAggregatorState &gstate, WindowAggregatorState &lstate, DataChunk &arg_chunk, idx_t input_idx,
	          optional_ptr<SelectionVector> filter_sel, idx_t filtered) override;
	void Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate, const FrameStats &stats) override;

	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate, const DataChunk &bounds,
	              Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowCustomAggregator : public WindowAggregator {
public:
	WindowCustomAggregator(AggregateObject aggr, const vector<LogicalType> &arg_types_p,
	                       const LogicalType &result_type_p, const WindowExcludeMode exclude_mode);
	~WindowCustomAggregator() override;

	unique_ptr<WindowAggregatorState> GetGlobalState(idx_t group_count,
	                                                 const ValidityMask &partition_mask) const override;
	void Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate, const FrameStats &stats) override;

	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate, const DataChunk &bounds,
	              Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowSegmentTree : public WindowAggregator {

public:
	WindowSegmentTree(AggregateObject aggr, const vector<LogicalType> &arg_types_p, const LogicalType &result_type_p,
	                  WindowAggregationMode mode_p, const WindowExcludeMode exclude_mode);

	unique_ptr<WindowAggregatorState> GetGlobalState(idx_t group_count,
	                                                 const ValidityMask &partition_mask) const override;
	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate, const FrameStats &stats) override;

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
	WindowDistinctAggregator(AggregateObject aggr, const vector<LogicalType> &arg_types_p,
	                         const LogicalType &result_type_p, const WindowExcludeMode exclude_mode_p,
	                         ClientContext &context);

	//	Build
	unique_ptr<WindowAggregatorState> GetGlobalState(idx_t group_count,
	                                                 const ValidityMask &partition_mask) const override;
	void Sink(WindowAggregatorState &gsink, WindowAggregatorState &lstate, DataChunk &arg_chunk, idx_t input_idx,
	          optional_ptr<SelectionVector> filter_sel, idx_t filtered) override;
	void Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate, const FrameStats &stats) override;

	//	Evaluate
	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate, const DataChunk &bounds,
	              Vector &result, idx_t count, idx_t row_idx) const override;

	//! Context for sorting
	ClientContext &context;
};

} // namespace duckdb
