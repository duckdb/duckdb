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
	static bool CanAggregate(const BoundWindowExpression &wexpr);

	WindowSegmentTree(const BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	unique_ptr<WindowAggregatorState> GetGlobalState(ClientContext &context, idx_t group_count,
	                                                 const ValidityMask &partition_mask) const override;
	unique_ptr<WindowAggregatorState> GetLocalState(const WindowAggregatorState &gstate) const override;
	void Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate, CollectionPtr collection,
	              const FrameStats &stats) override;

	void Evaluate(const WindowAggregatorState &gstate, WindowAggregatorState &lstate, const DataChunk &bounds,
	              Vector &result, idx_t count, idx_t row_idx) const override;
};

} // namespace duckdb
