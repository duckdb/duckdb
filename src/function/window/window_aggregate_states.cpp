#include "duckdb/function/window/window_aggregate_states.hpp"

namespace duckdb {

WindowAggregateStates::WindowAggregateStates(ClientContext &client, const AggregateObject &aggr)
    : client(client), aggr(aggr), state_size(aggr.function.state_size(aggr.function)),
      allocator(Allocator::Get(client)) {
}

void WindowAggregateStates::Initialize(idx_t count) {
	// Don't leak - every Initialize must be matched with a Destroy
	D_ASSERT(states.empty());

	states.resize(count * state_size);
	auto state_ptr = states.data();

	statef = make_uniq<Vector>(LogicalType::POINTER, count);
	auto state_f_data = FlatVector::GetData<data_ptr_t>(*statef);

	for (idx_t i = 0; i < count; ++i, state_ptr += state_size) {
		state_f_data[i] = state_ptr;
		aggr.function.initialize(aggr.function, state_ptr);
	}

	// Prevent conversion of results to constants
	statef->SetVectorType(VectorType::FLAT_VECTOR);
}

void WindowAggregateStates::Combine(WindowAggregateStates &target) {
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator, AggregateCombineType::ALLOW_DESTRUCTIVE);
	aggr.function.combine(*statef, *target.statef, aggr_input_data, GetCount());
}

void WindowAggregateStates::Finalize(Vector &result) {
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	aggr.function.finalize(*statef, aggr_input_data, result, GetCount(), 0);
}

void WindowAggregateStates::Destroy() {
	if (states.empty()) {
		return;
	}

	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	if (aggr.function.destructor) {
		aggr.function.destructor(*statef, aggr_input_data, GetCount());
	}

	states.clear();
}

} // namespace duckdb
