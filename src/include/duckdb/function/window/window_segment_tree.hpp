//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_aggregator.hpp"

namespace duckdb {

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

} // namespace duckdb
