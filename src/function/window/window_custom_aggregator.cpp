#include "duckdb/function/window/window_custom_aggregator.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

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

class WindowCustomAggregatorState : public WindowAggregatorLocalState {
public:
	WindowCustomAggregatorState(const AggregateObject &aggr, const WindowExcludeMode exclude_mode);
	~WindowCustomAggregatorState() override;

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
	explicit WindowCustomAggregatorGlobalState(ClientContext &context, const WindowCustomAggregator &aggregator,
	                                           idx_t group_count)
	    : WindowAggregatorGlobalState(context, aggregator, group_count), context(context) {

		gcstate = make_uniq<WindowCustomAggregatorState>(aggr, aggregator.exclude_mode);
	}

	//! Buffer manager for paging custom accelerator data
	ClientContext &context;
	//! Traditional packed filter mask for API
	ValidityMask filter_packed;
	//! Data pointer that contains a single local state, used for global custom window execution state
	unique_ptr<WindowCustomAggregatorState> gcstate;
	//! Partition description for custom window APIs
	unique_ptr<WindowPartitionInput> partition_input;
};

WindowCustomAggregatorState::WindowCustomAggregatorState(const AggregateObject &aggr,
                                                         const WindowExcludeMode exclude_mode)
    : aggr(aggr), state(aggr.function.state_size(aggr.function)),
      statef(Value::POINTER(CastPointerToValue(state.data()))), frames(3, {0, 0}) {
	// if we have a frame-by-frame method, share the single state
	aggr.function.initialize(aggr.function, state.data());

	InitSubFrames(frames, exclude_mode);
}

WindowCustomAggregatorState::~WindowCustomAggregatorState() {
	if (aggr.function.destructor) {
		AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
		aggr.function.destructor(statef, aggr_input_data, 1);
	}
}

unique_ptr<WindowAggregatorState> WindowCustomAggregator::GetGlobalState(ClientContext &context, idx_t group_count,
                                                                         const ValidityMask &) const {
	return make_uniq<WindowCustomAggregatorGlobalState>(context, *this, group_count);
}

void WindowCustomAggregator::Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate,
                                      CollectionPtr collection, const FrameStats &stats) {
	//	Single threaded Finalize for now
	auto &gcsink = gstate.Cast<WindowCustomAggregatorGlobalState>();
	lock_guard<mutex> gestate_guard(gcsink.lock);
	if (gcsink.finalized) {
		return;
	}

	WindowAggregator::Finalize(gstate, lstate, collection, stats);

	auto inputs = collection->inputs.get();
	const auto count = collection->size();
	vector<bool> all_valids;
	for (auto col_idx : child_idx) {
		all_valids.push_back(collection->all_valids[col_idx]);
	}
	auto &filter_mask = gcsink.filter_mask;
	auto &filter_packed = gcsink.filter_packed;
	filter_mask.Pack(filter_packed, filter_mask.Capacity());

	gcsink.partition_input =
	    make_uniq<WindowPartitionInput>(gcsink.context, inputs, count, child_idx, all_valids, filter_packed, stats);

	if (aggr.function.window_init) {
		auto &gcstate = *gcsink.gcstate;

		AggregateInputData aggr_input_data(aggr.GetFunctionData(), gcstate.allocator);
		aggr.function.window_init(aggr_input_data, *gcsink.partition_input, gcstate.state.data());
	}

	++gcsink.finalized;
}

unique_ptr<WindowAggregatorState> WindowCustomAggregator::GetLocalState(const WindowAggregatorState &gstate) const {
	return make_uniq<WindowCustomAggregatorState>(aggr, exclude_mode);
}

void WindowCustomAggregator::Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate,
                                      const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) const {
	auto &lcstate = lstate.Cast<WindowCustomAggregatorState>();
	auto &frames = lcstate.frames;
	const_data_ptr_t gstate_p = nullptr;
	auto &gcsink = gsink.Cast<WindowCustomAggregatorGlobalState>();
	if (gcsink.gcstate) {
		gstate_p = gcsink.gcstate->state.data();
	}

	EvaluateSubFrames(bounds, exclude_mode, count, row_idx, frames, [&](idx_t i) {
		// Extract the range
		AggregateInputData aggr_input_data(aggr.GetFunctionData(), lstate.allocator);
		aggr.function.window(aggr_input_data, *gcsink.partition_input, gstate_p, lcstate.state.data(), frames, result,
		                     i);
	});
}

} // namespace duckdb
