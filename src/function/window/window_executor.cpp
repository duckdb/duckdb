#include "duckdb/function/window/window_executor.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowExecutorBoundsLocalState
//===--------------------------------------------------------------------===//
WindowExecutorBoundsLocalState::WindowExecutorBoundsLocalState(ExecutionContext &context,
                                                               const WindowExecutorGlobalState &gstate)
    : WindowExecutorLocalState(context, gstate), partition_mask(gstate.partition_mask), order_mask(gstate.order_mask),
      state(gstate.executor.wexpr, gstate.payload_count) {
	vector<LogicalType> bounds_types(8, LogicalType(LogicalTypeId::UBIGINT));
	bounds.Initialize(Allocator::Get(context.client), bounds_types);
}

void WindowExecutorBoundsLocalState::UpdateBounds(WindowExecutorGlobalState &gstate, idx_t row_idx,
                                                  DataChunk &eval_chunk, optional_ptr<WindowCursor> range) {
	// Evaluate the row-level arguments
	WindowInputExpression boundary_start(eval_chunk, gstate.executor.boundary_start_idx);
	WindowInputExpression boundary_end(eval_chunk, gstate.executor.boundary_end_idx);

	const auto count = eval_chunk.size();
	state.Bounds(bounds, row_idx, range, count, boundary_start, boundary_end, partition_mask, order_mask);
}

//===--------------------------------------------------------------------===//
// WindowExecutor
//===--------------------------------------------------------------------===//
WindowExecutor::WindowExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
    : wexpr(wexpr),
      range_expr((WindowBoundariesState::HasPrecedingRange(wexpr) || WindowBoundariesState::HasFollowingRange(wexpr))
                     ? wexpr.orders[0].expression.get()
                     : nullptr) {
	if (range_expr) {
		range_idx = shared.RegisterCollection(wexpr.orders[0].expression, false);
	}

	boundary_start_idx = shared.RegisterEvaluate(wexpr.start_expr);
	boundary_end_idx = shared.RegisterEvaluate(wexpr.end_expr);
}

bool WindowExecutor::IgnoreNulls() const {
	return wexpr.ignore_nulls;
}

void WindowExecutor::Evaluate(ExecutionContext &context, idx_t row_idx, DataChunk &eval_chunk, Vector &result,
                              OperatorSinkInput &sink) const {
	auto &gbstate = sink.global_state.Cast<WindowExecutorGlobalState>();
	auto &lbstate = sink.local_state.Cast<WindowExecutorBoundsLocalState>();
	lbstate.UpdateBounds(gbstate, row_idx, eval_chunk, lbstate.range_cursor);

	const auto count = eval_chunk.size();
	EvaluateInternal(context, eval_chunk, result, count, row_idx, sink);

	result.Verify(count);
}

WindowExecutorGlobalState::WindowExecutorGlobalState(ClientContext &client, const WindowExecutor &executor,
                                                     const idx_t payload_count, const ValidityMask &partition_mask,
                                                     const ValidityMask &order_mask)
    : client(client), executor(executor), payload_count(payload_count), partition_mask(partition_mask),
      order_mask(order_mask) {
	for (const auto &child : executor.wexpr.children) {
		arg_types.emplace_back(child->return_type);
	}
}

WindowExecutorLocalState::WindowExecutorLocalState(ExecutionContext &context, const WindowExecutorGlobalState &gstate) {
}

void WindowExecutorLocalState::Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                    idx_t input_idx, OperatorSinkInput &sink) {
}

void WindowExecutorLocalState::Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink) {
	auto &gbstate = sink.global_state.Cast<WindowExecutorGlobalState>();
	const auto range_idx = gbstate.executor.range_idx;
	if (range_idx != DConstants::INVALID_INDEX) {
		range_cursor = make_uniq<WindowCursor>(*collection, range_idx);
	}
}

unique_ptr<GlobalSinkState> WindowExecutor::GetGlobalState(ClientContext &client, const idx_t payload_count,
                                                           const ValidityMask &partition_mask,
                                                           const ValidityMask &order_mask) const {
	return make_uniq<WindowExecutorGlobalState>(client, *this, payload_count, partition_mask, order_mask);
}

unique_ptr<LocalSinkState> WindowExecutor::GetLocalState(ExecutionContext &context,
                                                         const GlobalSinkState &gstate) const {
	return make_uniq<WindowExecutorBoundsLocalState>(context, gstate.Cast<WindowExecutorGlobalState>());
}

void WindowExecutor::Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk,
                          const idx_t input_idx, OperatorSinkInput &sink) const {
	auto &lbstate = sink.local_state.Cast<WindowExecutorBoundsLocalState>();
	lbstate.Sink(context, sink_chunk, coll_chunk, input_idx, sink);
}

void WindowExecutor::Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink) const {
	auto &lbstate = sink.local_state.Cast<WindowExecutorBoundsLocalState>();
	lbstate.Finalize(context, collection, sink);
}

} // namespace duckdb
