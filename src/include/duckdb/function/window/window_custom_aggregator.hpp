//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_custom_aggregator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_aggregator.hpp"

namespace duckdb {

class WindowCustomAggregator : public WindowAggregator {
public:
	static bool CanAggregate(const BoundWindowExpression &wexpr, WindowAggregationMode mode);

	WindowCustomAggregator(const BoundWindowExpression &wexpr, WindowSharedExpressions &shared);
	~WindowCustomAggregator() override;

	unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &context, idx_t group_count,
	                                           const ValidityMask &partition_mask) const override;
	void Finalize(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate, CollectionPtr collection,
	              const FrameStats &stats, InterruptState &interrupt) override;

	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;
	void Evaluate(ExecutionContext &context, const GlobalSinkState &gsink, LocalSinkState &lstate,
	              const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx,
	              InterruptState &interrupt) const override;
};

} // namespace duckdb
