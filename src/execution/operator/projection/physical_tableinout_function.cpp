#include "duckdb/execution/operator/projection/physical_tableinout_function.hpp"

namespace duckdb {

class TableInOutFunctionState : public OperatorState {
public:
	TableInOutFunctionState() {
	}

	unique_ptr<FunctionOperatorData> operator_data;
};

PhysicalTableInOutFunction::PhysicalTableInOutFunction(vector<LogicalType> types, TableFunction function_p,
                                                       unique_ptr<FunctionData> bind_data_p,
                                                       vector<column_t> column_ids_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::INOUT_FUNCTION, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data_p)), column_ids(move(column_ids_p)) {
}

unique_ptr<OperatorState> PhysicalTableInOutFunction::GetOperatorState(ClientContext &context) const {
	auto result = make_unique<TableInOutFunctionState>();
	if (function.init) {
		result->operator_data = function.init(context, bind_data.get(), column_ids, nullptr);
	}
	return move(result);
}

OperatorResultType PhysicalTableInOutFunction::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                       GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (TableInOutFunctionState &)state_p;
	return function.in_out_function(context.client, bind_data.get(), state.operator_data.get(), input, chunk);
}

} // namespace duckdb
