//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_constant_aggregator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_aggregator.hpp"

namespace duckdb {

class WindowConstantAggregator : public WindowAggregator {
public:
	static bool CanAggregate(const BoundWindowExpression &wexpr);

	static BoundWindowExpression &RebindAggregate(ClientContext &context, BoundWindowExpression &wexpr);

	WindowConstantAggregator(BoundWindowExpression &wexpr, WindowSharedExpressions &shared, ClientContext &context);
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

} // namespace duckdb
