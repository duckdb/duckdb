#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "core_functions/aggregate/nested_functions.hpp"

namespace duckdb {

namespace {

struct ListAggState {
	LinkedList linked_list;

	//! The state is a linked list of values - exported/imported as the aggregate's LIST return type
	using STATE_TYPE = StateListType<StateLayoutType::RETURN_TYPE>;
};

struct ListFunction {
	static bool IgnoreNull() {
		return false;
	}
};

void ListUpdateFunction(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &state_vector,
                        idx_t count) {
	D_ASSERT(input_count == 1);
	auto &input = inputs[0];
	RecursiveUnifiedVectorFormat input_data;
	Vector::RecursiveToUnifiedFormat(input, input_data);

	auto states = state_vector.Values<ListAggState *>();

	ListSegmentFunctions functions;
	GetSegmentDataFunctions(functions, input.GetType());

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i].GetValue();
		aggr_input_data.allocator.AlignNext();
		functions.AppendRow(aggr_input_data.allocator, state.linked_list, input_data, i);
	}
}

void ListAbsorbFunction(Vector &states_vector, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
	D_ASSERT(aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE);

	auto states = states_vector.Values<ListAggState *>();
	auto combined_ptr = FlatVector::GetDataMutable<ListAggState *>(combined);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i].GetValue();
		if (state.linked_list.total_capacity == 0) {
			// NULL, no need to append
			// this can happen when adding a FILTER to the grouping, e.g.,
			// LIST(i) FILTER (WHERE i <> 3)
			continue;
		}

		if (combined_ptr[i]->linked_list.total_capacity == 0) {
			combined_ptr[i]->linked_list = state.linked_list;
			continue;
		}

		// append the linked list
		combined_ptr[i]->linked_list.last_segment->next = state.linked_list.first_segment;
		combined_ptr[i]->linked_list.last_segment = state.linked_list.last_segment;
		combined_ptr[i]->linked_list.total_capacity += state.linked_list.total_capacity;
	}
}

void ListFinalize(Vector &states_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                  idx_t offset) {
	auto states = states_vector.Values<ListAggState *>();

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	ListSegmentFunctions functions;
	GetSegmentDataFunctions(functions, ListType::GetChildType(result.GetType()));

	vector<reference<const LinkedList>> linked_lists;
	linked_lists.reserve(count);
	for (idx_t i = 0; i < count; i++) {
		linked_lists.push_back(states[i].GetValue()->linked_list);
	}
	functions.BuildLists(linked_lists, result, offset);
}

void ListCombineFunction(Vector &states_vector, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
	//	Can we use destructive combining?
	if (aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
		ListAbsorbFunction(states_vector, combined, aggr_input_data, count);
		return;
	}

	auto states = states_vector.Values<ListAggState *>();
	auto combined_ptr = FlatVector::GetDataMutable<ListAggState *>(combined);

	auto child_type = ListType::GetChildType(aggr_input_data.function.GetReturnType());
	ListSegmentFunctions functions;
	GetSegmentDataFunctions(functions, child_type);

	for (idx_t i = 0; i < count; i++) {
		auto &source = *states[i].GetValue();
		auto &target = *combined_ptr[i];

		const auto entry_count = source.linked_list.total_capacity;
		Vector input(child_type, entry_count);
		functions.BuildListVector(source.linked_list, input, 0);

		RecursiveUnifiedVectorFormat input_data;
		Vector::RecursiveToUnifiedFormat(input, input_data);

		functions.AppendListEntry(aggr_input_data.allocator, target.linked_list, input_data,
		                          list_entry_t(0, entry_count));
	}
}

} // namespace

AggregateFunction ListFun::GetFunction() {
	auto func = AggregateFunction({LogicalType::TEMPLATE("T")}, LogicalType::LIST(LogicalType::TEMPLATE("T")),
	                              AggregateFunction::StateSize<ListAggState>,
	                              AggregateFunction::StateInitialize<ListAggState, ListFunction>, ListUpdateFunction,
	                              ListCombineFunction, ListFinalize, nullptr, nullptr, nullptr, nullptr);
	AggregateFunction::WireStructStateType<ListAggState>(func);

	return func;
}

} // namespace duckdb
