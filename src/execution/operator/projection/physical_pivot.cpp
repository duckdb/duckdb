#include "duckdb/execution/operator/projection/physical_pivot.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

PhysicalPivot::PhysicalPivot(vector<LogicalType> types_p, PhysicalOperator &child, BoundPivotInfo bound_pivot_p)
    : PhysicalOperator(PhysicalOperatorType::PIVOT, std::move(types_p), child.estimated_cardinality),
      bound_pivot(std::move(bound_pivot_p)) {
	children.push_back(child);
	for (idx_t p = 0; p < bound_pivot.pivot_values.size(); p++) {
		auto entry = pivot_map.find(bound_pivot.pivot_values[p]);
		if (entry != pivot_map.end()) {
			continue;
		}
		pivot_map[bound_pivot.pivot_values[p]] = bound_pivot.group_count + p;
	}
	// extract the empty aggregate expressions
	ArenaAllocator allocator(Allocator::DefaultAllocator());
	for (auto &aggr_expr : bound_pivot.aggregates) {
		auto &aggr = aggr_expr->Cast<BoundAggregateExpression>();
		// for each aggregate, initialize an empty aggregate state and finalize it immediately
		auto state = make_unsafe_uniq_array<data_t>(aggr.function.state_size(aggr.function));
		aggr.function.initialize(aggr.function, state.get());
		Vector state_vector(Value::POINTER(CastPointerToValue(state.get())));
		Vector result_vector(aggr_expr->return_type);
		AggregateInputData aggr_input_data(aggr.bind_info.get(), allocator);
		aggr.function.finalize(state_vector, aggr_input_data, result_vector, 1, 0);
		empty_aggregates.push_back(result_vector.GetValue(0));
	}
}

OperatorResultType PhysicalPivot::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                          GlobalOperatorState &gstate, OperatorState &state) const {
	// copy the groups as-is
	input.Flatten();
	for (idx_t i = 0; i < bound_pivot.group_count; i++) {
		chunk.data[i].Reference(input.data[i]);
	}
	auto pivot_column_lists = FlatVector::GetData<list_entry_t>(input.data.back());
	auto &pivot_column_values = ListVector::GetEntry(input.data.back());
	auto pivot_columns = FlatVector::GetData<string_t>(pivot_column_values);

	// initialize all aggregate columns with the empty aggregate value
	// if there are multiple aggregates the columns are in order of [AGGR1][AGGR2][AGGR1][AGGR2]
	// so we need to alternate the empty_aggregate that we use
	idx_t aggregate = 0;
	for (idx_t c = bound_pivot.group_count; c < chunk.ColumnCount(); c++) {
		chunk.data[c].Reference(empty_aggregates[aggregate]);
		chunk.data[c].Flatten(input.size());
		aggregate++;
		if (aggregate >= empty_aggregates.size()) {
			aggregate = 0;
		}
	}

	// move the pivots to the given columns
	for (idx_t r = 0; r < input.size(); r++) {
		auto list = pivot_column_lists[r];
		for (idx_t l = 0; l < list.length; l++) {
			// figure out the column value number of this list
			auto &column_name = pivot_columns[list.offset + l];
			auto entry = pivot_map.find(column_name);
			if (entry == pivot_map.end()) {
				// column entry not found in map - that means this element is explicitly excluded from the pivot list
				continue;
			}
			auto column_idx = entry->second;
			for (idx_t aggr = 0; aggr < empty_aggregates.size(); aggr++) {
				auto pivot_value_lists = FlatVector::GetData<list_entry_t>(input.data[bound_pivot.group_count + aggr]);
				auto &pivot_value_child = ListVector::GetEntry(input.data[bound_pivot.group_count + aggr]);
				if (list.length != pivot_value_lists[r].length) {
					throw InternalException("Pivot - unaligned lists between values and columns!?");
				}
				chunk.data[column_idx + aggr].SetValue(r, pivot_value_child.GetValue(pivot_value_lists[r].offset + l));
			}
		}
	}
	chunk.SetCardinality(input.size());
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
