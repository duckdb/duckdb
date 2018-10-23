
#include "execution/operator/physical_table_function.hpp"
#include "execution/expression_executor.hpp"

#include "catalog/catalog_entry/schema_catalog_entry.hpp"

#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

vector<string> PhysicalTableFunction::GetNames() {
	vector<string> names;
	for (auto &column : function->return_values) {
		names.push_back(column.name);
	}
	return names;
}

vector<TypeId> PhysicalTableFunction::GetTypes() {
	vector<TypeId> types;
	for (auto &column : function->return_values) {
		types.push_back(column.type);
	}
	return types;
}

void PhysicalTableFunction::_GetChunk(ClientContext &context, DataChunk &chunk,
                                      PhysicalOperatorState *state_) {
	auto state = (PhysicalTableFunctionOperatorState *)state_;

	chunk.Reset();

	if (!state->initialized) {
		// run initialization code
		if (function->init) {
			auto function_data = function->init(context);
			if (function_data) {
				state->function_data = unique_ptr<TableFunctionData>(function_data);
			}
		}
		state->initialized = true;
	}
	// create the input arguments
	DataChunk input;
	input.Initialize(function->arguments);

	ExpressionExecutor executor(nullptr, context);
	executor.Execute(function_call->children, input);

	// run main code
	function->function(context, input, chunk, state->function_data.get());
	if (chunk.count == 0) {
		// finished, call clean up
		if (function->final) {
			function->final(context, state->function_data.get());
		}
	}
}

unique_ptr<PhysicalOperatorState>
PhysicalTableFunction::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalTableFunctionOperatorState>(parent_executor);
}
