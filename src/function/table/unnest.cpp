#include "duckdb/function/table/range.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

struct UnnestFunctionData : public TableFunctionData {
	explicit UnnestFunctionData(Value value) : value(move(value)) {
	}

	Value value;
};

struct UnnestOperatorData : public FunctionOperatorData {
	UnnestOperatorData() : current_count(0) {
	}

	idx_t current_count;
};

static unique_ptr<FunctionData> UnnestBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto &inputs = input.inputs;
	return_types.push_back(ListType::GetChildType(inputs[0].type()));
	names.push_back(inputs[0].ToString());
	return make_unique<UnnestFunctionData>(inputs[0]);
}

static unique_ptr<FunctionOperatorData> UnnestInit(ClientContext &context, const FunctionData *bind_data,
                                                   const vector<column_t> &column_ids, TableFilterCollection *filters) {
	return make_unique<UnnestOperatorData>();
}

static void UnnestFunction(ClientContext &context, const FunctionData *bind_data_p,
                           FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
	auto &bind_data = (UnnestFunctionData &)*bind_data_p;
	auto &state = (UnnestOperatorData &)*operator_state;

	auto &list_value = ListValue::GetChildren(bind_data.value);
	idx_t count = 0;
	for (; state.current_count < list_value.size() && count < STANDARD_VECTOR_SIZE; state.current_count++) {
		output.data[0].SetValue(count, list_value[state.current_count]);
		count++;
	}
	output.SetCardinality(count);
}

void UnnestTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction unnest_function("unnest", {LogicalTypeId::LIST}, UnnestFunction, UnnestBind, UnnestInit);
	set.AddFunction(unnest_function);
}

} // namespace duckdb
