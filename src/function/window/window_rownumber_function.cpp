#include "duckdb/function/window/window_rownumber_function.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/function/window/window_token_tree.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowRowNumberGlobalState
//===--------------------------------------------------------------------===//
class WindowRowNumberGlobalState : public WindowExecutorGlobalState {
public:
	WindowRowNumberGlobalState(ClientContext &client, const WindowRowNumberExecutor &executor,
	                           const idx_t payload_count, const ValidityMask &partition_mask,
	                           const ValidityMask &order_mask)
	    : WindowExecutorGlobalState(client, executor, payload_count, partition_mask, order_mask),
	      ntile_idx(executor.ntile_idx) {
		if (!executor.arg_order_idx.empty()) {
			use_framing = true;

			//	If the argument order is prefix of the partition ordering,
			//	then we can just use the partition ordering.
			auto &wexpr = executor.wexpr;
			auto &arg_orders = executor.wexpr.arg_orders;
			const auto optimize = ClientConfig::GetConfig(client).enable_optimizer;
			if (!optimize || BoundWindowExpression::GetSharedOrders(wexpr.orders, arg_orders) != arg_orders.size()) {
				//	"The ROW_NUMBER function can be computed by disambiguating duplicate elements based on their
				//	position in the input data, such that two elements never compare as equal."
				token_tree = make_uniq<WindowTokenTree>(client, executor.wexpr.arg_orders, executor.arg_order_idx,
				                                        payload_count, true);
			}
		}
	}

	//! Use framing instead of partitions (ORDER BY arguments)
	bool use_framing = false;

	//! The token tree for ORDER BY arguments
	unique_ptr<WindowTokenTree> token_tree;

	//! The evaluation index for NTILE
	const column_t ntile_idx;
};

//===--------------------------------------------------------------------===//
// WindowRowNumberLocalState
//===--------------------------------------------------------------------===//
class WindowRowNumberLocalState : public WindowExecutorBoundsLocalState {
public:
	explicit WindowRowNumberLocalState(ExecutionContext &context, const WindowRowNumberGlobalState &grstate)
	    : WindowExecutorBoundsLocalState(context, grstate), grstate(grstate) {
		if (grstate.token_tree) {
			local_tree = grstate.token_tree->GetLocalState(context);
		}
	}

	//! Accumulate the secondary sort values
	void Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t input_idx,
	          OperatorSinkInput &sink) override;
	//! Finish the sinking and prepare to scan
	void Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink) override;

	//! The corresponding global peer state
	const WindowRowNumberGlobalState &grstate;
	//! The optional sorting state for secondary sorts
	unique_ptr<LocalSinkState> local_tree;
};

void WindowRowNumberLocalState::Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                     idx_t input_idx, OperatorSinkInput &sink) {
	WindowExecutorBoundsLocalState::Sink(context, sink_chunk, coll_chunk, input_idx, sink);

	if (local_tree) {
		auto &local_tokens = local_tree->Cast<WindowMergeSortTreeLocalState>();
		local_tokens.Sink(context, sink_chunk, input_idx, nullptr, 0, sink.interrupt_state);
	}
}

void WindowRowNumberLocalState::Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink) {
	WindowExecutorBoundsLocalState::Finalize(context, collection, sink);

	if (local_tree) {
		auto &local_tokens = local_tree->Cast<WindowMergeSortTreeLocalState>();
		local_tokens.Finalize(context, sink.interrupt_state);
		local_tokens.window_tree.Build();
	}
}

//===--------------------------------------------------------------------===//
// WindowRowNumberExecutor
//===--------------------------------------------------------------------===//
WindowRowNumberExecutor::WindowRowNumberExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, shared) {
	for (const auto &order : wexpr.arg_orders) {
		arg_order_idx.emplace_back(shared.RegisterSink(order.expression));
	}
}

unique_ptr<GlobalSinkState> WindowRowNumberExecutor::GetGlobalState(ClientContext &client, const idx_t payload_count,
                                                                    const ValidityMask &partition_mask,
                                                                    const ValidityMask &order_mask) const {
	return make_uniq<WindowRowNumberGlobalState>(client, *this, payload_count, partition_mask, order_mask);
}

unique_ptr<LocalSinkState> WindowRowNumberExecutor::GetLocalState(ExecutionContext &context,
                                                                  const GlobalSinkState &gstate) const {
	return make_uniq<WindowRowNumberLocalState>(context, gstate.Cast<WindowRowNumberGlobalState>());
}

void WindowRowNumberExecutor::EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result,
                                               idx_t count, idx_t row_idx, OperatorSinkInput &sink) const {
	auto &grstate = sink.global_state.Cast<WindowRowNumberGlobalState>();
	auto &lrstate = sink.local_state.Cast<WindowRowNumberLocalState>();
	auto rdata = FlatVector::GetData<int64_t>(result);

	if (grstate.use_framing) {
		auto frame_begin = FlatVector::GetData<const idx_t>(lrstate.bounds.data[FRAME_BEGIN]);
		auto frame_end = FlatVector::GetData<const idx_t>(lrstate.bounds.data[FRAME_END]);
		if (grstate.token_tree) {
			for (idx_t i = 0; i < count; ++i, ++row_idx) {
				// Row numbers are unique ranks
				rdata[i] = UnsafeNumericCast<int64_t>(grstate.token_tree->Rank(frame_begin[i], frame_end[i], row_idx));
			}
		} else {
			for (idx_t i = 0; i < count; ++i, ++row_idx) {
				rdata[i] = UnsafeNumericCast<int64_t>(row_idx - frame_begin[i] + 1);
			}
		}
		return;
	}

	auto partition_begin = FlatVector::GetData<const idx_t>(lrstate.bounds.data[PARTITION_BEGIN]);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		rdata[i] = UnsafeNumericCast<int64_t>(row_idx - partition_begin[i] + 1);
	}
}

//===--------------------------------------------------------------------===//
// WindowNtileExecutor
//===--------------------------------------------------------------------===//
WindowNtileExecutor::WindowNtileExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
    : WindowRowNumberExecutor(wexpr, shared) {
	// NTILE has one argument
	ntile_idx = shared.RegisterEvaluate(wexpr.children[0]);
}

void WindowNtileExecutor::EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result,
                                           idx_t count, idx_t row_idx, OperatorSinkInput &sink) const {
	auto &grstate = sink.global_state.Cast<WindowRowNumberGlobalState>();
	auto &lrstate = sink.local_state.Cast<WindowRowNumberLocalState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lrstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lrstate.bounds.data[PARTITION_END]);
	if (grstate.use_framing) {
		// With secondary sorts, we restrict to the frame boundaries, but everything else should compute the same.
		partition_begin = FlatVector::GetData<const idx_t>(lrstate.bounds.data[FRAME_BEGIN]);
		partition_end = FlatVector::GetData<const idx_t>(lrstate.bounds.data[FRAME_END]);
	}
	auto rdata = FlatVector::GetData<int64_t>(result);
	WindowInputExpression ntile_col(eval_chunk, ntile_idx);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		if (ntile_col.CellIsNull(i)) {
			FlatVector::SetNull(result, i, true);
		} else {
			auto n_param = ntile_col.GetCell<int64_t>(i);
			if (n_param < 1) {
				throw InvalidInputException("Argument for ntile must be greater than zero");
			}
			// With thanks from SQLite's ntileValueFunc()
			auto n_total = NumericCast<int64_t>(partition_end[i] - partition_begin[i]);
			if (n_param > n_total) {
				// more groups allowed than we have values
				// map every entry to a unique group
				n_param = n_total;
			}
			int64_t n_size = (n_total / n_param);
			// find the row idx within the group
			D_ASSERT(row_idx >= partition_begin[i]);
			idx_t partition_idx = 0;
			if (grstate.token_tree) {
				partition_idx = grstate.token_tree->Rank(partition_begin[i], partition_end[i], row_idx) - 1;
			} else {
				partition_idx = row_idx - partition_begin[i];
			}
			auto adjusted_row_idx = NumericCast<int64_t>(partition_idx);

			// now compute the ntile
			int64_t n_large = n_total - n_param * n_size;
			int64_t i_small = n_large * (n_size + 1);
			int64_t result_ntile;

			D_ASSERT((n_large * (n_size + 1) + (n_param - n_large) * n_size) == n_total);

			if (adjusted_row_idx < i_small) {
				result_ntile = 1 + adjusted_row_idx / (n_size + 1);
			} else {
				result_ntile = 1 + n_large + (adjusted_row_idx - i_small) / n_size;
			}
			// result has to be between [1, NTILE]
			D_ASSERT(result_ntile >= 1 && result_ntile <= n_param);
			rdata[i] = result_ntile;
		}
	}
}

} // namespace duckdb
