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
		vector<LogicalType> input_types;
		auto &child_types = children[0]->types;
		idx_t input_length = child_types.size() - projected_input.size();
		for (idx_t k = 0; k < input_length; k++) {
			input_types.push_back(child_types[k]);
		}
		for (idx_t k = 0; k < projected_input.size(); k++) {
			D_ASSERT(projected_input[k] >= input_length);
		}
		result->input_chunk.Initialize(context.client, input_types);
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

OperatorResultType PhysicalTableInOutFunction::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                       GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<TableInOutGlobalState>();
	auto &state = state_p.Cast<TableInOutLocalState>();
	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	if (projected_input.empty()) {
		// straightforward case - no need to project input
		return function.in_out_function(context, data, input, chunk);
	}
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
		// set up the input data to the table in-out function
		for (idx_t col_idx = 0; col_idx < state.input_chunk.ColumnCount(); col_idx++) {
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

InsertionOrderPreservingMap<string> PhysicalTableInOutFunction::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	if (function.to_string) {
		result["__text__"] = function.to_string(bind_data.get());
	} else {
		result["Name"] = function.name;
	}
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

OperatorFinalizeResultType PhysicalTableInOutFunction::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                                    GlobalOperatorState &gstate_p,
                                                                    OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<TableInOutGlobalState>();
	auto &state = state_p.Cast<TableInOutLocalState>();
	if (!projected_input.empty()) {
		throw InternalException("FinalExecute not supported for project_input");
	}
	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	return function.in_out_function_final(context, data, chunk);
}

} // namespace duckdb
