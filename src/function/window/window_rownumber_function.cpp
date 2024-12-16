#include "duckdb/function/window/window_rownumber_function.hpp"
#include "duckdb/function/window/window_token_tree.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowRowNumberGlobalState
//===--------------------------------------------------------------------===//
class WindowRowNumberGlobalState : public WindowExecutorGlobalState {
public:
	WindowRowNumberGlobalState(const WindowRowNumberExecutor &executor, const idx_t payload_count,
	                           const ValidityMask &partition_mask, const ValidityMask &order_mask)
	    : WindowExecutorGlobalState(executor, payload_count, partition_mask, order_mask) {
		if (!executor.arg_order_idx.empty()) {
			//	"The ROW_NUMBER function can be computed by disambiguating duplicate elements based on their position in
			//	the input data, such that two elements never compare as equal."
			token_tree = make_uniq<WindowTokenTree>(executor.context, executor.wexpr.arg_orders, executor.arg_order_idx,
			                                        payload_count, true);
		}
	}

	//! The token tree for ORDER BY arguments
	unique_ptr<WindowTokenTree> token_tree;
};

//===--------------------------------------------------------------------===//
// WindowRowNumberLocalState
//===--------------------------------------------------------------------===//
class WindowRowNumberLocalState : public WindowExecutorBoundsState {
public:
	explicit WindowRowNumberLocalState(const WindowRowNumberGlobalState &grstate)
	    : WindowExecutorBoundsState(grstate), grstate(grstate) {
		if (grstate.token_tree) {
			local_tree = grstate.token_tree->GetLocalState();
		}
	}

	//! Accumulate the secondary sort values
	void Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk,
	          idx_t input_idx) override;
	//! Finish the sinking and prepare to scan
	void Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) override;

	//! The corresponding global peer state
	const WindowRowNumberGlobalState &grstate;
	//! The optional sorting state for secondary sorts
	unique_ptr<WindowAggregatorState> local_tree;
};

void WindowRowNumberLocalState::Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                     idx_t input_idx) {
	WindowExecutorBoundsState::Sink(gstate, sink_chunk, coll_chunk, input_idx);

	if (local_tree) {
		auto &local_tokens = local_tree->Cast<WindowMergeSortTreeLocalState>();
		local_tokens.SinkChunk(sink_chunk, input_idx, nullptr, 0);
	}
}

void WindowRowNumberLocalState::Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) {
	WindowExecutorBoundsState::Finalize(gstate, collection);

	if (local_tree) {
		auto &local_tokens = local_tree->Cast<WindowMergeSortTreeLocalState>();
		local_tokens.Sort();
		local_tokens.window_tree.Build();
	}
}

//===--------------------------------------------------------------------===//
// WindowRowNumberExecutor
//===--------------------------------------------------------------------===//
WindowRowNumberExecutor::WindowRowNumberExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorGlobalState> WindowRowNumberExecutor::GetGlobalState(const idx_t payload_count,
                                                                              const ValidityMask &partition_mask,
                                                                              const ValidityMask &order_mask) const {
	return make_uniq<WindowRowNumberGlobalState>(*this, payload_count, partition_mask, order_mask);
}

unique_ptr<WindowExecutorLocalState>
WindowRowNumberExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowRowNumberLocalState>(gstate.Cast<WindowRowNumberGlobalState>());
}

void WindowRowNumberExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
	auto &grstate = gstate.Cast<WindowRowNumberGlobalState>();
	auto &lrstate = lstate.Cast<WindowRowNumberLocalState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lrstate.bounds.data[PARTITION_BEGIN]);
	auto rdata = FlatVector::GetData<uint64_t>(result);

	if (grstate.token_tree) {
		auto partition_end = FlatVector::GetData<const idx_t>(lrstate.bounds.data[PARTITION_END]);
		for (idx_t i = 0; i < count; ++i, ++row_idx) {
			// Row numbers are unique ranks
			rdata[i] = grstate.token_tree->Rank(partition_begin[i], partition_end[i], row_idx);
		}
		return;
	}

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		rdata[i] = row_idx - partition_begin[i] + 1;
	}
}

} // namespace duckdb
