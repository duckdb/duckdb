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
	WindowPeerExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	unique_ptr<WindowExecutorGlobalState> GetGlobalState(ClientContext &context, const idx_t payload_count,
	                                                     const ValidityMask &partition_mask,
	                                                     const ValidityMask &order_mask) const override;
	unique_ptr<WindowExecutorLocalState> GetLocalState(ExecutionContext &context,
	                                                   const WindowExecutorGlobalState &gstate) const override;

	//! The column indices of any ORDER BY argument expressions
	vector<column_t> arg_order_idx;
};

class WindowRankExecutor : public WindowPeerExecutor {
public:
	WindowRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(ExecutionContext &context, WindowExecutorGlobalState &gstate,
	                      WindowExecutorLocalState &lstate, DataChunk &eval_chunk, Vector &result, idx_t count,
	                      idx_t row_idx) const override;
};

class WindowDenseRankExecutor : public WindowPeerExecutor {
public:
	WindowDenseRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(ExecutionContext &context, WindowExecutorGlobalState &gstate,
	                      WindowExecutorLocalState &lstate, DataChunk &eval_chunk, Vector &result, idx_t count,
	                      idx_t row_idx) const override;
};

class WindowPercentRankExecutor : public WindowPeerExecutor {
public:
	WindowPercentRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(ExecutionContext &context, WindowExecutorGlobalState &gstate,
	                      WindowExecutorLocalState &lstate, DataChunk &eval_chunk, Vector &result, idx_t count,
	                      idx_t row_idx) const override;
};

class WindowCumeDistExecutor : public WindowPeerExecutor {
public:
	WindowCumeDistExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(ExecutionContext &context, WindowExecutorGlobalState &gstate,
	                      WindowExecutorLocalState &lstate, DataChunk &eval_chunk, Vector &result, idx_t count,
	                      idx_t row_idx) const override;
};

} // namespace duckdb
