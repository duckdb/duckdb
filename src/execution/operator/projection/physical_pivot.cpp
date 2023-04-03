#include "duckdb/execution/operator/projection/physical_pivot.hpp"


namespace duckdb {

PhysicalPivot::PhysicalPivot(vector<LogicalType> types_p, unique_ptr<PhysicalOperator> child, vector<PivotValueElement> pivot_values_p, idx_t group_count) :
	PhysicalOperator(PhysicalOperatorType::PIVOT, std::move(types_p), child->estimated_cardinality), group_count(group_count), pivot_values(std::move(pivot_values_p)) {
	children.push_back(std::move(child));
	for(idx_t p = 0; p < pivot_values.size(); p++) {
		pivot_map[pivot_values[p].name] = group_count + p;
	}
	empty_aggregate = Value(types[1]);
}

OperatorResultType PhysicalPivot::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
						   GlobalOperatorState &gstate, OperatorState &state) const  {
	// copy the groups as-is
	for(idx_t i = 0; i < group_count; i++) {
		chunk.data[i].Reference(input.data[i]);
	}
	auto pivot_value_lists = FlatVector::GetData<list_entry_t>(input.data[group_count]);
	auto &pivot_value_children = ListVector::GetEntry(input.data[group_count]);
	auto pivot_column_lists = FlatVector::GetData<list_entry_t>(input.data[group_count + 1]);
	auto &pivot_column_values = ListVector::GetEntry(input.data[group_count + 1]);
	auto pivot_columns = FlatVector::GetData<string_t>(pivot_column_values);

	// move the pivots to the given columns
	for(idx_t r = 0; r < input.size(); r++) {
		auto list = pivot_value_lists[r];
		if (list.offset != pivot_column_lists[r].offset || list.length != pivot_column_lists[r].length) {
			throw InternalException("Pivot - unaligned lists between values and columns!?");
		}
		for(idx_t c = group_count; c < chunk.ColumnCount(); c++) {
			chunk.data[c].SetValue(r, empty_aggregate);
		}
		for(idx_t l = 0; l < list.length; l++) {
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


}
