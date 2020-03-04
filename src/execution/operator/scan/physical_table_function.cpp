#include "duckdb/execution/operator/scan/physical_table_function.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

class PhysicalTableFunctionOperatorState : public PhysicalOperatorState {
public:
	PhysicalTableFunctionOperatorState() : PhysicalOperatorState(nullptr), initialized(false) {
	}

	unique_ptr<FunctionData> function_data;
	bool initialized;
};

void PhysicalTableFunction::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = (PhysicalTableFunctionOperatorState *)state_;
	if (!state->initialized) {
		// run initialization code
		if (function->function.init) {
			state->function_data = function->function.init(context);
		}
		state->initialized = true;
	}
	// create the input arguments
	vector<TypeId> input_types;
	for (auto &argument_type : function->function.arguments) {
		input_types.push_back(GetInternalType(argument_type));
	}

	DataChunk input;
	if (parameters.size() > 0) {
		assert(parameters.size() == input_types.size());
		input.Initialize(input_types);

		for (auto &expr : parameters) {
			ExpressionExecutor executor(*expr);
			executor.Execute(input);
		}
	}

	// run main code
	function->function.function(context, input, chunk, state->function_data.get());
	if (chunk.size() == 0) {
		// finished, call clean up
		if (function->function.final) {
			function->function.final(context, state->function_data.get());
		}
	}
}

unique_ptr<PhysicalOperatorState> PhysicalTableFunction::GetOperatorState() {
	return make_unique<PhysicalTableFunctionOperatorState>();
}
