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

	unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &context, idx_t group_count,
	                                           const ValidityMask &partition_mask) const override;
	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;
	void Finalize(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate, CollectionPtr collection,
	              const FrameStats &stats, InterruptState &interrupt) override;

	void Evaluate(ExecutionContext &context, const GlobalSinkState &gstate, LocalSinkState &lstate,
	              const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx,
	              InterruptState &interrupt) const override;
};

} // namespace duckdb
