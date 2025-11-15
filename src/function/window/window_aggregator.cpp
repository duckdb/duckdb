#include "duckdb/function/window/window_aggregator.hpp"

#include "duckdb/function/window/window_collection.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowAggregator
//===--------------------------------------------------------------------===//
WindowAggregator::WindowAggregator(const BoundWindowExpression &wexpr)
    : wexpr(wexpr), aggr(wexpr), result_type(wexpr.return_type), state_size(aggr.function.state_size(aggr.function)),
      exclude_mode(wexpr.exclude_clause) {
	for (auto &child : wexpr.children) {
		arg_types.emplace_back(child->return_type);
	}
}

WindowAggregator::WindowAggregator(const BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
    : WindowAggregator(wexpr) {
	for (auto &child : wexpr.children) {
		child_idx.emplace_back(shared.RegisterCollection(child, false));
	}
}

WindowAggregator::~WindowAggregator() {
}

WindowAggregatorGlobalState::WindowAggregatorGlobalState(ClientContext &client, const WindowAggregator &aggregator_p,
                                                         idx_t group_count)
    : client(client), allocator(BufferAllocator::Get(client)), aggregator(aggregator_p), aggr(aggregator.wexpr),
      locals(0), finalized(0) {
	if (aggr.filter) {
		// 	Start with all invalid and set the ones that pass
		filter_mask.Initialize(group_count, false);
	} else {
		filter_mask.InitializeEmpty(group_count);
	}
}

unique_ptr<GlobalSinkState> WindowAggregator::GetGlobalState(ClientContext &context, idx_t group_count,
                                                             const ValidityMask &) const {
	return make_uniq<WindowAggregatorGlobalState>(context, *this, group_count);
}

WindowAggregatorLocalState::WindowAggregatorLocalState(ExecutionContext &context)
    : allocator(BufferAllocator::Get(context.client)) {
}

void WindowAggregatorLocalState::Sink(ExecutionContext &context, WindowAggregatorGlobalState &gastate,
                                      DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t input_idx) {
}

void WindowAggregator::Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t input_idx,
                            optional_ptr<SelectionVector> filter_sel, idx_t filtered, OperatorSinkInput &sink) {
	auto &gastate = sink.global_state.Cast<WindowAggregatorGlobalState>();
	auto &lastate = sink.local_state.Cast<WindowAggregatorLocalState>();
	lastate.Sink(context, gastate, sink_chunk, coll_chunk, input_idx);
	if (filter_sel) {
		auto &filter_mask = gastate.filter_mask;
		for (idx_t f = 0; f < filtered; ++f) {
			filter_mask.SetValid(input_idx + filter_sel->get_index(f));
		}
	}
}

void WindowAggregatorLocalState::InitSubFrames(SubFrames &frames, const WindowExcludeMode exclude_mode) {
	idx_t nframes = 0;
	switch (exclude_mode) {
	case WindowExcludeMode::NO_OTHER:
		nframes = 1;
		break;
	case WindowExcludeMode::TIES:
		nframes = 3;
		break;
	case WindowExcludeMode::CURRENT_ROW:
	case WindowExcludeMode::GROUP:
		nframes = 2;
		break;
	}
	frames.resize(nframes, {0, 0});
}

void WindowAggregatorLocalState::Finalize(ExecutionContext &context, WindowAggregatorGlobalState &gastate,
                                          CollectionPtr collection) {
	// Prepare to scan
	if (!cursor) {
		cursor = make_uniq<WindowCursor>(*collection, gastate.aggregator.child_idx);
	}
}

void WindowAggregator::Finalize(ExecutionContext &context, CollectionPtr collection, const FrameStats &stats,
                                OperatorSinkInput &sink) {
	auto &gasink = sink.global_state.Cast<WindowAggregatorGlobalState>();
	auto &lastate = sink.local_state.Cast<WindowAggregatorLocalState>();
	lastate.Finalize(context, gasink, collection);
}

} // namespace duckdb
