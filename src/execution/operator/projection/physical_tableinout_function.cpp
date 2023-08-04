#include "duckdb/execution/operator/projection/physical_tableinout_function.hpp"

namespace duckdb {

class TableInOutLocalState : public OperatorState {
public:
	TableInOutLocalState() : row_index(0), new_row(true) {
	}

	unique_ptr<LocalTableFunctionState> local_state;
	idx_t row_index;
	bool new_row;
	DataChunk input_chunk;
	unique_ptr<DataChunk> intermediate_chunk;

public:
	DataChunk &GetIntermediateChunk(DataChunk &output, idx_t base_column_count) {
		if (!intermediate_chunk) {
			intermediate_chunk = make_uniq<DataChunk>();
			// Create an empty DataChunk that has room for the input + the mapping vector
			auto chunk_types = output.GetTypes();
			vector<LogicalType> intermediate_types;
			intermediate_types.reserve(base_column_count + 1);
			intermediate_types.insert(intermediate_types.end(), chunk_types.begin(),
			                          chunk_types.begin() + base_column_count);
			intermediate_types.emplace_back(LogicalType::UINTEGER);
			// We initialize this as empty
			intermediate_chunk->InitializeEmpty(intermediate_types);
			// And only allocate for our mapping vector
			intermediate_chunk->data[base_column_count].Initialize();
		}
		// Initialize our output chunk
		for (idx_t i = 0; i < base_column_count; i++) {
			intermediate_chunk->data[i].Reference(output.data[i]);
		}
		intermediate_chunk->SetCardinality(output.size());
		return *intermediate_chunk;
	}
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
	auto &gstate = op_state->Cast<TableInOutGlobalState>();
	auto result = make_uniq<TableInOutLocalState>();
	if (function.init_local) {
		TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
		result->local_state = function.init_local(context, input, gstate.global_state.get());
	}
	if (!projected_input.empty()) {
		if (!function.in_out_mapping) {
			// If we have to project columns, and the function doesn't provide a mapping from output row -> input row
			// then we have to execute tuple-at-a-time
			result->input_chunk.Initialize(context.client, children[0]->types);
		}
	}
	return std::move(result);
}

unique_ptr<GlobalOperatorState> PhysicalTableInOutFunction::GetGlobalOperatorState(ClientContext &context) const {
	auto result = make_uniq<TableInOutGlobalState>();
	if (function.init_global) {
		TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
		result->global_state = function.init_global(context, input);
	}
	return std::move(result);
}

void PhysicalTableInOutFunction::AddProjectedColumnsFromConstantMapping(idx_t map_idx, DataChunk &input,
                                                                        DataChunk &intermediate, DataChunk &out) const {
	auto &mapping_column = intermediate.data[map_idx];
	D_ASSERT(mapping_column.GetVectorType() == VectorType::CONSTANT_VECTOR);

	auto mapping_data = FlatVector::GetData<sel_t>(mapping_column);

	// Add the projected columns, and apply the selection vector
	for (idx_t project_idx = 0; project_idx < projected_input.size(); project_idx++) {
		auto source_idx = projected_input[project_idx];
		D_ASSERT(source_idx < input.data.size());
		auto target_idx = map_idx + project_idx;

		auto &target_column = out.data[target_idx];
		auto &source_column = input.data[source_idx];

		ConstantVector::Reference(target_column, source_column, mapping_data[0], input.size());
	}
}

OperatorResultType PhysicalTableInOutFunction::ExecuteWithMapping(ExecutionContext &context, DataChunk &input,
                                                                  DataChunk &chunk, TableInOutLocalState &state,
                                                                  TableFunctionInput &data) const {
	// Create a duplicate of 'chunk' that contains one extra column
	// this column is used to register the relation between input tuple -> output tuple(s)
	const auto base_columns = chunk.ColumnCount() - projected_input.size();

	auto &intermediate_chunk = state.GetIntermediateChunk(chunk, base_columns);

	// Let the function know that we expect it to write an in-out mapping for rowids
	data.add_in_out_mapping = true;
	auto result = function.in_out_function(context, data, input, intermediate_chunk);
	chunk.SetCardinality(intermediate_chunk.size());

	// Move the result into the output chunk
	for (idx_t i = 0; i < base_columns; i++) {
		chunk.data[i].Reference(intermediate_chunk.data[i]);
	}

	auto &mapping_column = intermediate_chunk.data[base_columns];
	switch (mapping_column.GetVectorType()) {
	case VectorType::CONSTANT_VECTOR: {
		// We can avoid creating a selection vector altogether
		AddProjectedColumnsFromConstantMapping(base_columns, input, intermediate_chunk, chunk);
		break;
	}
	default: {
		throw NotImplementedException(
		    "Executing Table in-out functions with a non-constant mapping is not supported yet");
	}
	}

	return result;
}

OperatorResultType PhysicalTableInOutFunction::ExecuteWithoutMapping(ExecutionContext &context, DataChunk &input,
                                                                     DataChunk &chunk, TableInOutGlobalState &gstate,
                                                                     TableInOutLocalState &state,
                                                                     TableFunctionInput &data) const {
	// when project_input is set we execute the input function row-by-row
	if (state.new_row) {
		if (state.row_index >= input.size()) {
			// finished processing this chunk
			state.new_row = true;
			state.row_index = 0;
			return OperatorResultType::NEED_MORE_INPUT;
		}
		// we are processing a new row: fetch the data for the current row
		state.input_chunk.Reset();
		D_ASSERT(input.ColumnCount() == state.input_chunk.ColumnCount());
		// set up the input data to the table in-out function
		for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
			ConstantVector::Reference(state.input_chunk.data[col_idx], input.data[col_idx], state.row_index, 1);
		}
		state.input_chunk.SetCardinality(1);
		state.row_index++;
		state.new_row = false;
	}
	// set up the output data in "chunk"
	D_ASSERT(chunk.ColumnCount() > projected_input.size());
	D_ASSERT(state.row_index > 0);
	idx_t base_idx = chunk.ColumnCount() - projected_input.size();
	for (idx_t project_idx = 0; project_idx < projected_input.size(); project_idx++) {
		auto source_idx = projected_input[project_idx];
		auto target_idx = base_idx + project_idx;
		ConstantVector::Reference(chunk.data[target_idx], input.data[source_idx], state.row_index - 1, 1);
	}
	auto result = function.in_out_function(context, data, state.input_chunk, chunk);
	if (result == OperatorResultType::FINISHED) {
		return result;
	}
	if (result == OperatorResultType::NEED_MORE_INPUT) {
		// we finished processing this row: move to the next row
		state.new_row = true;
	}
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

OperatorResultType PhysicalTableInOutFunction::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                       GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<TableInOutGlobalState>();
	auto &state = state_p.Cast<TableInOutLocalState>();
	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	if (projected_input.empty()) {
		return function.in_out_function(context, data, input, chunk);
	}
	if (function.in_out_mapping) {
		return ExecuteWithMapping(context, input, chunk, state, data);
	}
	return ExecuteWithoutMapping(context, input, chunk, gstate, state, data);
}

OperatorFinalizeResultType PhysicalTableInOutFunction::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                                    GlobalOperatorState &gstate_p,
                                                                    OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<TableInOutGlobalState>();
	auto &state = state_p.Cast<TableInOutLocalState>();
	if (!projected_input.empty()) {
		throw InternalException("FinalExecute not supported for project_input");
	}
	D_ASSERT(RequiresFinalExecute());
	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	return function.in_out_function_final(context, data, chunk);
}

} // namespace duckdb
