#include "duckdb/function/window/window_custom_aggregator.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/common/enums/window_aggregation_mode.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowCustomAggregator
//===--------------------------------------------------------------------===//
bool WindowCustomAggregator::CanAggregate(const BoundWindowExpression &wexpr, WindowAggregationMode mode) {
	if (!wexpr.aggregate) {
		return false;
	}

	if (!wexpr.aggregate->window) {
		return false;
	}

	//	ORDER BY arguments are not currently supported
	if (!wexpr.arg_orders.empty()) {
		return false;
	}

	return (mode < WindowAggregationMode::COMBINE);
}

WindowCustomAggregator::WindowCustomAggregator(const BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
    : WindowAggregator(wexpr, shared) {
}

WindowCustomAggregator::~WindowCustomAggregator() {
}

class WindowCustomAggregatorLocalState : public WindowAggregatorLocalState {
public:
	WindowCustomAggregatorLocalState(const AggregateObject &aggr, const WindowExcludeMode exclude_mode);
	~WindowCustomAggregatorLocalState() override;

public:
	//! The aggregate function
	const AggregateObject aggr;
	//! Data pointer that contains a single state, shared by all the custom evaluators
	vector<data_t> state;
	//! Reused result state container for the window functions
	Vector statef;
	//! The frame boundaries, used for the window functions
	SubFrames frames;
};

class WindowCustomAggregatorGlobalState : public WindowAggregatorGlobalState {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	WindowCustomAggregatorGlobalState(ClientContext &client, const WindowCustomAggregator &aggregator,
	                                  idx_t group_count)
	    : WindowAggregatorGlobalState(client, aggregator, group_count) {
		gcstate = make_uniq<WindowCustomAggregatorLocalState>(aggr, aggregator.exclude_mode);
	}

	//! Traditional packed filter mask for API
	ValidityMask filter_packed;
	//! Data pointer that contains a single local state, used for global custom window execution state
	unique_ptr<WindowCustomAggregatorLocalState> gcstate;
	//! The argument data
	CollectionPtr collection;
	//! Column global validity flags
	vector<bool> all_valids;
	//! Frame statistics
	FrameStats stats;
};

WindowCustomAggregatorLocalState::WindowCustomAggregatorLocalState(const AggregateObject &aggr,
                                                                   const WindowExcludeMode exclude_mode)
    : aggr(aggr), state(aggr.function.state_size(aggr.function)),
      statef(Value::POINTER(CastPointerToValue(state.data()))), frames(3, {0, 0}) {
	// if we have a frame-by-frame method, share the single state
	aggr.function.initialize(aggr.function, state.data());

	InitSubFrames(frames, exclude_mode);
}

WindowCustomAggregatorLocalState::~WindowCustomAggregatorLocalState() {
	if (aggr.function.destructor) {
		AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
		aggr.function.destructor(statef, aggr_input_data, 1);
	}
}

unique_ptr<WindowAggregatorState> WindowCustomAggregator::GetGlobalState(ClientContext &context, idx_t group_count,
                                                                         const ValidityMask &) const {
	return make_uniq<WindowCustomAggregatorGlobalState>(context, *this, group_count);
}

void WindowCustomAggregator::Finalize(ExecutionContext &context, WindowAggregatorState &gstate,
                                      WindowAggregatorState &lstate, CollectionPtr collection, const FrameStats &stats,
                                      InterruptState &interrupt) {
	//	Single threaded Finalize for now
	auto &gcsink = gstate.Cast<WindowCustomAggregatorGlobalState>();
	lock_guard<mutex> gestate_guard(gcsink.lock);
	if (gcsink.finalized) {
		return;
	}

	WindowAggregator::Finalize(context, gstate, lstate, collection, stats, interrupt);

	gcsink.collection = collection;
	auto inputs = collection->inputs.get();
	const auto count = collection->size();
	auto &all_valids = gcsink.all_valids;
	for (auto col_idx : child_idx) {
		all_valids.push_back(collection->all_valids[col_idx]);
	}
	auto &filter_mask = gcsink.filter_mask;
	auto &filter_packed = gcsink.filter_packed;
	filter_mask.Pack(filter_packed, filter_mask.Capacity());

	if (aggr.function.window_init) {
		auto &gcstate = *gcsink.gcstate;
		WindowPartitionInput partition(context, inputs, count, child_idx, all_valids, filter_packed, stats, interrupt);

		AggregateInputData aggr_input_data(aggr.GetFunctionData(), gcstate.allocator);
		aggr.function.window_init(aggr_input_data, partition, gcstate.state.data());
	}

	++gcsink.finalized;
}

unique_ptr<WindowAggregatorState> WindowCustomAggregator::GetLocalState(const WindowAggregatorState &gstate) const {
	return make_uniq<WindowCustomAggregatorLocalState>(aggr, exclude_mode);
}

void WindowCustomAggregator::Evaluate(ExecutionContext &context, const WindowAggregatorState &gsink,
                                      WindowAggregatorState &lstate, const DataChunk &bounds, Vector &result,
                                      idx_t count, idx_t row_idx, InterruptState &interrupt) const {
	auto &lcstate = lstate.Cast<WindowCustomAggregatorLocalState>();
	auto &frames = lcstate.frames;
	const_data_ptr_t gstate_p = nullptr;
	auto &gcsink = gsink.Cast<WindowCustomAggregatorGlobalState>();
	if (gcsink.gcstate) {
		gstate_p = gcsink.gcstate->state.data();
	}

	auto collection = gcsink.collection;
	auto inputs = collection->inputs.get();
	auto &all_valids = gcsink.all_valids;
	auto &filter_packed = gcsink.filter_packed;
	auto &stats = gcsink.stats;
	WindowPartitionInput partition(context, inputs, collection->size(), child_idx, all_valids, filter_packed, stats,
	                               interrupt);
	EvaluateSubFrames(bounds, exclude_mode, count, row_idx, frames, [&](idx_t i) {
		// Extract the range
		AggregateInputData aggr_input_data(aggr.GetFunctionData(), lstate.allocator);
		aggr.function.window(aggr_input_data, partition, gstate_p, lcstate.state.data(), frames, result, i);
	});
}

} // namespace duckdb
