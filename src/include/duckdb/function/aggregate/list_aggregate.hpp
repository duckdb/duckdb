//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/list_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

//! The state of the "list" aggregate - shared by aggregates that buffer their input in a linked list
struct ListAggState {
	LinkedList linked_list;

	using STATE_TYPE = StateListType<StateReturnType>;
};

struct ListFunction {
	static bool IgnoreNull() {
		return false;
	}

	static LogicalType GetElementType(AggregateInputData &aggr_input_data) {
		return ListType::GetChildType(aggr_input_data.function.GetReturnType());
	}
};

//! Appends the i-th input row to the i-th state's linked list.
//! When IGNORE_NULLS is set, NULL input rows are not appended.
template <bool IGNORE_NULLS = false>
inline void ListUpdateFunction(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                               Vector &state_vector, idx_t count) {
	D_ASSERT(input_count == 1);
	auto &input = inputs[0];
	RecursiveUnifiedVectorFormat input_data;
	Vector::RecursiveToUnifiedFormat(input, input_data);

	auto states = state_vector.Values<ListAggState *>();

	ListSegmentFunctions functions;
	GetSegmentDataFunctions(functions, input.GetType());

	for (idx_t i = 0; i < count; i++) {
		if (IGNORE_NULLS) {
			const auto idx = input_data.unified.sel->get_index(i);
			if (!input_data.unified.validity.RowIsValid(idx)) {
				continue;
			}
		}
		auto &state = *states[i].GetValue();
		aggr_input_data.allocator.AlignNext();
		functions.AppendRow(aggr_input_data.allocator, state.linked_list, input_data, i);
	}
}

inline void ListAbsorbFunction(Vector &states_vector, Vector &combined, AggregateInputData &aggr_input_data,
                               idx_t count) {
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

//! OP provides GetElementType(aggr_input_data), returning the type of the values stored in the linked list
template <class OP>
void ListCombineFunction(Vector &states_vector, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
	//	Can we use destructive combining?
	if (aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
		ListAbsorbFunction(states_vector, combined, aggr_input_data, count);
		return;
	}

	auto states = states_vector.Values<ListAggState *>();
	auto combined_ptr = FlatVector::GetDataMutable<ListAggState *>(combined);

	auto element_type = OP::GetElementType(aggr_input_data);
	ListSegmentFunctions functions;
	GetSegmentDataFunctions(functions, element_type);

	for (idx_t i = 0; i < count; i++) {
		auto &source = *states[i].GetValue();
		auto &target = *combined_ptr[i];

		const auto entry_count = source.linked_list.total_capacity;
		Vector input(element_type, entry_count);
		functions.BuildListVector(source.linked_list, input, 0);

		RecursiveUnifiedVectorFormat input_data;
		Vector::RecursiveToUnifiedFormat(input, input_data);

		functions.AppendListEntry(aggr_input_data.allocator, target.linked_list, input_data,
		                          list_entry_t(0, entry_count));
	}
}

} // namespace duckdb
