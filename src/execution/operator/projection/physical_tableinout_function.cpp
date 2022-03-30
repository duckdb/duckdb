#include "duckdb/execution/operator/projection/physical_tableinout_function.hpp"

namespace duckdb {

class TableInOutFunctionState : public OperatorState {
public:
	TableInOutFunctionState() : initialized(false) {
	}

	unique_ptr<FunctionOperatorData> operator_data;
	bool initialized = false;
};

PhysicalTableInOutFunction::PhysicalTableInOutFunction(vector<LogicalType> types, TableFunction function_p,
                                                       unique_ptr<FunctionData> bind_data_p,
                                                       vector<column_t> column_ids_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::INOUT_FUNCTION, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data_p)), column_ids(move(column_ids_p)) {
}

unique_ptr<OperatorState> PhysicalTableInOutFunction::GetOperatorState(ClientContext &context) const {
	return make_unique<TableInOutFunctionState>();
}

OperatorResultType PhysicalTableInOutFunction::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                       GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (TableInOutFunctionState &)state_p;

	if (!state.initialized) {
		if (function.init) {
			state.operator_data = function.init(context.client, bind_data.get(), column_ids, nullptr);
		}
		state.initialized = true;
	}

	function.function(context.client, bind_data.get(), state.operator_data.get(), &input, chunk);
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
