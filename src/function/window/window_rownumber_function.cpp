#include "duckdb/function/window/window_rownumber_function.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowRowNumberExecutor
//===--------------------------------------------------------------------===//
WindowRowNumberExecutor::WindowRowNumberExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

void WindowRowNumberExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
	auto &lbstate = lstate.Cast<WindowExecutorBoundsState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		rdata[i] = NumericCast<int64_t>(row_idx - partition_begin[i] + 1);
	}
}

} // namespace duckdb
