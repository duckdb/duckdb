#include "duckdb/function/table/range.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

struct RepeatFunctionData : public TableFunctionData {
	RepeatFunctionData(Value value, idx_t target_count) : value(move(value)), target_count(target_count) {
	}

	Value value;
	idx_t target_count;
};

struct RepeatOperatorData : public FunctionOperatorData {
	RepeatOperatorData() : current_count(0) {
	}
	idx_t current_count;
};

static unique_ptr<FunctionData> RepeatBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	// the repeat function returns the type of the first argument
	auto &inputs = input.inputs;
	return_types.push_back(inputs[0].type());
	names.push_back(inputs[0].ToString());
	return make_unique<RepeatFunctionData>(inputs[0], inputs[1].GetValue<int64_t>());
}

static unique_ptr<FunctionOperatorData> RepeatInit(ClientContext &context, const FunctionData *bind_data,
                                                   const vector<column_t> &column_ids, TableFilterCollection *filters) {
	return make_unique<RepeatOperatorData>();
}

static void RepeatFunction(ClientContext &context, const FunctionData *bind_data_p,
                           FunctionOperatorData *operator_state, DataChunk &output) {
	auto &bind_data = (RepeatFunctionData &)*bind_data_p;
	auto &state = (RepeatOperatorData &)*operator_state;

	idx_t remaining = MinValue<idx_t>(bind_data.target_count - state.current_count, STANDARD_VECTOR_SIZE);
	output.data[0].Reference(bind_data.value);
	output.SetCardinality(remaining);
	state.current_count += remaining;
}

static unique_ptr<NodeStatistics> RepeatCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (RepeatFunctionData &)*bind_data_p;
	return make_unique<NodeStatistics>(bind_data.target_count, bind_data.target_count);
}

void RepeatTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction repeat("repeat", {LogicalType::ANY, LogicalType::BIGINT}, RepeatFunction, RepeatBind, RepeatInit,
	                     nullptr, nullptr, nullptr, RepeatCardinality);
	set.AddFunction(repeat);
}

} // namespace duckdb
