#include "duckdb/function/window/window_executor.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

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

	if (wexpr.window) {
		if (wexpr.window->HasSharingCallback()) {
			wexpr.window->GetSharing(*this, shared);
		} else {
			//	If no one overrides, assume the arguments are only needed at evaluate time
			for (auto &child : wexpr.children) {
				child_idx.emplace_back(shared.RegisterEvaluate(child));
			}
		}
	}
}

void WindowExecutor::Evaluate(ExecutionContext &context, idx_t row_idx, DataChunk &eval_chunk, Vector &result,
                              OperatorSinkInput &sink) const {
	auto &lbstate = sink.local_state.Cast<WindowExecutorLocalState>();
	lbstate.state.UpdateBounds(row_idx, eval_chunk);

	EvaluateInternal(context, eval_chunk, lbstate.state.bounds, result, row_idx, sink);

	result.Verify(eval_chunk.size());
}

WindowExecutorGlobalState::WindowExecutorGlobalState(ClientContext &client, const WindowExecutor &executor,
                                                     const idx_t payload_count, const ValidityMask &partition_mask,
                                                     const ValidityMask &order_mask)
    : client(client), executor(executor), payload_count(payload_count), partition_mask(partition_mask),
      order_mask(order_mask) {
	for (const auto &child : executor.wexpr.children) {
		arg_types.emplace_back(child->GetReturnType());
	}
}

WindowExecutorLocalState::WindowExecutorLocalState(ExecutionContext &context, const WindowExecutorGlobalState &gstate)
    : state(context, gstate) {
}

void WindowExecutorLocalState::Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                    idx_t input_idx, OperatorSinkInput &sink) {
}

void WindowExecutorLocalState::Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink) {
}

unique_ptr<GlobalSinkState> WindowExecutor::GetGlobalState(ClientContext &client, const idx_t payload_count,
                                                           const ValidityMask &partition_mask,
                                                           const ValidityMask &order_mask) const {
	if (wexpr.window && wexpr.window->HasGlobalCallback()) {
		return wexpr.window->GetGlobalState(client, *this, payload_count, partition_mask, order_mask);
	}
	return make_uniq<WindowExecutorGlobalState>(client, *this, payload_count, partition_mask, order_mask);
}

unique_ptr<LocalSinkState> WindowExecutor::GetLocalState(ExecutionContext &context,
                                                         const GlobalSinkState &gstate) const {
	if (wexpr.window && wexpr.window->HasLocalCallback()) {
		return wexpr.window->GetLocalState(context, gstate);
	}
	return make_uniq<WindowExecutorLocalState>(context, gstate.Cast<WindowExecutorGlobalState>());
}

void WindowExecutor::Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk,
                          const idx_t input_idx, OperatorSinkInput &sink) const {
	if (wexpr.window && wexpr.window->HasSinkCallback()) {
		wexpr.window->Sink(context, sink_chunk, coll_chunk, input_idx, sink);
	} else {
		auto &lbstate = sink.local_state.Cast<WindowExecutorLocalState>();
		lbstate.Sink(context, sink_chunk, coll_chunk, input_idx, sink);
	}
}

void WindowExecutor::Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink) const {
	auto &lbstate = sink.local_state.Cast<WindowExecutorLocalState>();
	lbstate.state.Finalize(collection);

	if (wexpr.window && wexpr.window->HasFinalizeCallback()) {
		wexpr.window->Finalize(context, collection, sink);
	} else {
		lbstate.Finalize(context, collection, sink);
	}
}

void WindowExecutor::EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds,
                                      Vector &result, idx_t row_idx, OperatorSinkInput &sink) const {
	if (wexpr.window && wexpr.window->HasEvaluateCallback()) {
		wexpr.window->Evaluate(context, eval_chunk, bounds, result, row_idx, sink);
	}
}

} // namespace duckdb
