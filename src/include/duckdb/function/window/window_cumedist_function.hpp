//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_cumedist_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_executor.hpp"

namespace duckdb {

class WindowCumeDistExecutor : public WindowExecutor {
public:
	WindowCumeDistExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

} // namespace duckdb
