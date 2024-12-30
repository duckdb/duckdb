//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_rank_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_executor.hpp"

namespace duckdb {

class WindowPeerExecutor : public WindowExecutor {
public:
	WindowPeerExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

	unique_ptr<WindowExecutorGlobalState> GetGlobalState(const idx_t payload_count, const ValidityMask &partition_mask,
	                                                     const ValidityMask &order_mask) const override;
	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;
};

class WindowRankExecutor : public WindowPeerExecutor {
public:
	WindowRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowDenseRankExecutor : public WindowPeerExecutor {
public:
	WindowDenseRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowPercentRankExecutor : public WindowPeerExecutor {
public:
	WindowPercentRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

} // namespace duckdb
