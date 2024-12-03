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

class WindowRankExecutor : public WindowExecutor {
public:
	WindowRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowDenseRankExecutor : public WindowExecutor {
public:
	WindowDenseRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowPercentRankExecutor : public WindowExecutor {
public:
	WindowPercentRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

} // namespace duckdb
