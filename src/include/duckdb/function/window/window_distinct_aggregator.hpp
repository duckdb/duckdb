//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_distinct_aggregator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_aggregator.hpp"

namespace duckdb {

class WindowDistinctAggregator : public WindowAggregator {
public:
	static bool CanAggregate(const BoundWindowExpression &wexpr);

	WindowDistinctAggregator(const BoundWindowExpression &wexpr, WindowSharedExpressions &shared,
	                         ClientContext &client);

	//	Build
	unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &client, idx_t group_count,
	                                           const ValidityMask &partition_mask) const override;
	void Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate, DataChunk &sink_chunk,
	          DataChunk &coll_chunk, idx_t input_idx, optional_ptr<SelectionVector> filter_sel, idx_t filtered,
	          InterruptState &interrupt) override;
	void Finalize(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate, CollectionPtr collection,
	              const FrameStats &stats, InterruptState &interrupt) override;

	//	Evaluate
	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const override;
	void Evaluate(ExecutionContext &context, const GlobalSinkState &gsink, LocalSinkState &lstate,
	              const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx,
	              InterruptState &interrupt) const override;

	//! Context for sorting
	ClientContext &context;
};

} // namespace duckdb
