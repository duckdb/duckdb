//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_aggregate_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_executor.hpp"
#include "duckdb/common/enums/window_aggregation_mode.hpp"
#include "duckdb/function/window/window_aggregator.hpp"

namespace duckdb {

class WindowAggregateExecutor : public WindowExecutor {
public:
	WindowAggregateExecutor(BoundWindowExpression &wexpr, ClientContext &client, WindowSharedExpressions &shared,
	                        WindowAggregationMode mode);

	void Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk, const idx_t input_idx,
	          WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate) const override;
	void Finalize(ExecutionContext &context, WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
	              CollectionPtr collection) const override;

	unique_ptr<WindowExecutorGlobalState> GetGlobalState(ClientContext &client, const idx_t payload_count,
	                                                     const ValidityMask &partition_mask,
	                                                     const ValidityMask &order_mask) const override;
	unique_ptr<WindowExecutorLocalState> GetLocalState(ExecutionContext &context,
	                                                   const WindowExecutorGlobalState &gstate) const override;

	const WindowAggregationMode mode;

	// aggregate computation algorithm
	unique_ptr<WindowAggregator> aggregator;

	// FILTER reference expression in sink_chunk
	unique_ptr<Expression> filter_ref;

protected:
	void EvaluateInternal(ExecutionContext &context, WindowExecutorGlobalState &gstate,
	                      WindowExecutorLocalState &lstate, DataChunk &eval_chunk, Vector &result, idx_t count,
	                      idx_t row_idx) const override;
};

} // namespace duckdb
