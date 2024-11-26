//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_segment_tree.hpp
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
#include "duckdb/function/window/window_aggregator.hpp"

namespace duckdb {

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
