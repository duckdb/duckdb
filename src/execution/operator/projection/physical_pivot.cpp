#include "duckdb/execution/operator/projection/physical_pivot.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

PhysicalPivot::PhysicalPivot(vector<LogicalType> types_p, unique_ptr<PhysicalOperator> child,
                             BoundPivotInfo bound_pivot_p)
    : PhysicalOperator(PhysicalOperatorType::PIVOT, std::move(types_p), child->estimated_cardinality),
      bound_pivot(std::move(bound_pivot_p)) {
	children.push_back(std::move(child));
	for (idx_t p = 0; p < bound_pivot.pivot_values.size(); p++) {
		pivot_map[bound_pivot.pivot_values[p]] = bound_pivot.group_count + p;
	}
	// extract the empty aggregate expressions
	for (auto &aggr_expr : bound_pivot.aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggr_expr;
		// for each aggregate, initialize an empty aggregate state and finalize it immediately
		auto state = unique_ptr<data_t[]>(new data_t[aggr.function.state_size()]);
		aggr.function.initialize(state.get());
		Vector state_vector(Value::POINTER((uintptr_t)state.get()));
		Vector result_vector(aggr_expr->return_type);
		AggregateInputData aggr_input_data(aggr.bind_info.get(), Allocator::DefaultAllocator());
		aggr.function.finalize(state_vector, aggr_input_data, result_vector, 1, 0);
		empty_aggregates.push_back(result_vector.GetValue(0));
	}
}

OperatorResultType PhysicalPivot::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                          GlobalOperatorState &gstate, OperatorState &state) const {
	// copy the groups as-is
	for (idx_t i = 0; i < bound_pivot.group_count; i++) {
		chunk.data[i].Reference(input.data[i]);
	}
	auto pivot_value_lists = FlatVector::GetData<list_entry_t>(input.data[bound_pivot.group_count]);
	auto &pivot_value_children = ListVector::GetEntry(input.data[bound_pivot.group_count]);
	auto pivot_column_lists = FlatVector::GetData<list_entry_t>(input.data[bound_pivot.group_count + 1]);
	auto &pivot_column_values = ListVector::GetEntry(input.data[bound_pivot.group_count + 1]);
	auto pivot_columns = FlatVector::GetData<string_t>(pivot_column_values);

	// initialize all aggregate columns with the empty aggregate value
	for (idx_t c = bound_pivot.group_count; c < chunk.ColumnCount(); c++) {
		chunk.data[c].Reference(empty_aggregates[0]);
		chunk.data[c].Flatten(input.size());
	}

	// move the pivots to the given columns
	for (idx_t r = 0; r < input.size(); r++) {
		auto list = pivot_value_lists[r];
		if (list.offset != pivot_column_lists[r].offset || list.length != pivot_column_lists[r].length) {
			throw InternalException("Pivot - unaligned lists between values and columns!?");
		}
		for (idx_t l = 0; l < list.length; l++) {
			// figure out the column value number of this list
			auto &column_name = pivot_columns[list.offset + l];
			auto entry = pivot_map.find(column_name);
			if (entry == pivot_map.end()) {
				// column entry not found in map - that means this element is explicitly excluded from the pivot list
				continue;
			}
			auto column_idx = entry->second;
			chunk.data[column_idx].SetValue(r, pivot_value_children.GetValue(list.offset + l));
		}
	}
	chunk.SetCardinality(input.size());
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
