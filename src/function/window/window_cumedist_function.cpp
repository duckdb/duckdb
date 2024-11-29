#include "duckdb/function/window/window_cumedist_function.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowCumeDistExecutor
//===--------------------------------------------------------------------===//
WindowCumeDistExecutor::WindowCumeDistExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                               WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

void WindowCumeDistExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                              DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &lbstate = lstate.Cast<WindowExecutorBoundsState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_END]);
	auto peer_end = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PEER_END]);
	auto rdata = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		auto denom = static_cast<double>(NumericCast<int64_t>(partition_end[i] - partition_begin[i]));
		double cume_dist = denom > 0 ? ((double)(peer_end[i] - partition_begin[i])) / denom : 0;
		rdata[i] = cume_dist;
	}
}

} // namespace duckdb
