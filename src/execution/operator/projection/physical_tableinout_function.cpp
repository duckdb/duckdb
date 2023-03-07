#include "duckdb/execution/operator/projection/physical_tableinout_function.hpp"

namespace duckdb {

class TableInOutLocalState : public OperatorState {
public:
	TableInOutLocalState() {
	}

	unique_ptr<LocalTableFunctionState> local_state;
	DataChunk input_chunk;
};

class TableInOutGlobalState : public GlobalOperatorState {
public:
	TableInOutGlobalState() {
	}

	unique_ptr<GlobalTableFunctionState> global_state;
};

PhysicalTableInOutFunction::PhysicalTableInOutFunction(vector<LogicalType> types, TableFunction function_p,
                                                       unique_ptr<FunctionData> bind_data_p,
                                                       vector<column_t> column_ids_p, idx_t estimated_cardinality,
                                                       vector<column_t> project_input_p)
    : PhysicalOperator(PhysicalOperatorType::INOUT_FUNCTION, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data_p)), column_ids(std::move(column_ids_p)),
      projected_input(std::move(project_input_p)) {
}

unique_ptr<OperatorState> PhysicalTableInOutFunction::GetOperatorState(ExecutionContext &context) const {
	auto &gstate = (TableInOutGlobalState &)*op_state;
	auto result = make_unique<TableInOutLocalState>();
	if (function.init_local) {
		TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
		result->local_state = function.init_local(context, input, gstate.global_state.get());
	}
	if (!projected_input.empty()) {
		result->input_chunk.Initialize(context.client, children[0]->types);
	}
	return std::move(result);
}

unique_ptr<GlobalOperatorState> PhysicalTableInOutFunction::GetGlobalOperatorState(ClientContext &context) const {
	auto result = make_unique<TableInOutGlobalState>();
	if (function.init_global) {
		TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
		result->global_state = function.init_global(context, input);
	}
	return std::move(result);
}

OperatorResultType PhysicalTableInOutFunction::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                       GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = (TableInOutGlobalState &)gstate_p;
	auto &state = (TableInOutLocalState &)state_p;
	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	if (projected_input.empty()) {
		// straightforward case - no need to project input
		return function.in_out_function(context, data, input, chunk);
	}

	// Create a duplicate of 'chunk' that contains one extra column
	// this column is used to register the relation between input tuple -> output tuple(s)
	DataChunk intermediate_chunk;
	vector<LogicalType> intermediate_types = chunk.GetTypes();
	intermediate_types.push_back(LogicalType::USMALLINT);
	intermediate_chunk.InitializeEmpty(intermediate_types);

	const auto base_columns = chunk.ColumnCount() - projected_input.size();
	for (idx_t i = 0; i < base_columns; i++) {
		intermediate_chunk.data[i].Reference(chunk.data[i]);
	}
	intermediate_chunk.data[base_columns].Initialize();
	intermediate_chunk.SetCardinality(chunk.size());

	auto result = function.in_out_function(context, data, input, intermediate_chunk);
	chunk.SetCardinality(intermediate_chunk.size());

	for (idx_t i = 0; i < base_columns; i++) {
		chunk.data[i].Reference(intermediate_chunk.data[i]);
	}

	auto &mapping_column = intermediate_chunk.data[base_columns];
	D_ASSERT(mapping_column.GetVectorType() == VectorType::FLAT_VECTOR);
	UnifiedVectorFormat mapping_data;
	mapping_column.ToUnifiedFormat(intermediate_chunk.size(), mapping_data);
	D_ASSERT(mapping_data.validity.AllValid());
	auto mapping_array = (uint32_t *)mapping_data.data;

	SelectionVector sel_vec;
	sel_vec.Initialize(intermediate_chunk.size());
	// Create a selection vector that maps from output row -> input row
	for (idx_t i = 0; i < intermediate_chunk.size(); i++) {
		// The index in the input column that produced this output tuple
		const auto input_row = mapping_array[i];
		D_ASSERT(input_row < STANDARD_VECTOR_SIZE);
		sel_vec.set_index(0, input_row);
	}

	// Add the projected columns, and apply the selection vector
	for (idx_t project_idx = 0; project_idx < projected_input.size(); project_idx++) {
		auto source_idx = projected_input[project_idx];
		D_ASSERT(source_idx < input.data.size());
		auto target_idx = base_columns + project_idx;

		auto &target_column = chunk.data[target_idx];
		auto &source_column = input.data[source_idx];

		// Reference the original
		target_column.Reference(source_column);
		// And slice, to rearrange which index points to which tuple
		target_column.Slice(sel_vec, intermediate_chunk.size());
	}

	return result;
}

OperatorFinalizeResultType PhysicalTableInOutFunction::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                                    GlobalOperatorState &gstate_p,
                                                                    OperatorState &state_p) const {
	auto &gstate = (TableInOutGlobalState &)gstate_p;
	auto &state = (TableInOutLocalState &)state_p;
	if (!projected_input.empty()) {
		throw InternalException("FinalExecute not supported for project_input");
	}
	D_ASSERT(RequiresFinalExecute());
	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	return function.in_out_function_final(context, data, chunk);
}

} // namespace duckdb
