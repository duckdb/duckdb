#include "duckdb/function/table/range.hpp"
#include "duckdb/common/algorithm.hpp"

using namespace std;

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

static unique_ptr<FunctionData> repeat_bind(ClientContext &context, vector<Value> &inputs,
                                            unordered_map<string, Value> &named_parameters,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	// the repeat function returns the type of the first argument
	return_types.push_back(inputs[0].type());
	names.push_back(inputs[0].ToString());
	return make_unique<RepeatFunctionData>(inputs[0], inputs[1].GetValue<int64_t>());
}

static unique_ptr<FunctionOperatorData> repeat_init(ClientContext &context, const FunctionData *bind_data,
                                                    vector<column_t> &column_ids, TableFilterSet *table_filters) {
	return make_unique<RepeatOperatorData>();
}

static void repeat_function(ClientContext &context, const FunctionData *bind_data_,
                            FunctionOperatorData *operator_state, DataChunk &output) {
	auto &bind_data = (RepeatFunctionData &)*bind_data_;
	auto &state = (RepeatOperatorData &)*operator_state;

	idx_t remaining = min<idx_t>(bind_data.target_count - state.current_count, STANDARD_VECTOR_SIZE);
	output.data[0].Reference(bind_data.value);
	output.SetCardinality(remaining);
	state.current_count += remaining;
}

static idx_t repeat_cardinality(const FunctionData *bind_data_) {
	auto &bind_data = (RepeatFunctionData &)*bind_data_;
	return bind_data.target_count;
}

void RepeatTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction repeat("repeat", {LogicalType::ANY, LogicalType::BIGINT}, repeat_function, repeat_bind, repeat_init,
	                     nullptr, nullptr, repeat_cardinality);
	set.AddFunction(repeat);
}

} // namespace duckdb
