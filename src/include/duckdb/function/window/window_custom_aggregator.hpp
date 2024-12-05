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

} // namespace duckdb
