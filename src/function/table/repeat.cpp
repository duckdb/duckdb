#include "duckdb/function/table/range.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

struct RepeatFunctionData : public TableFunctionData {
	RepeatFunctionData(Value value, idx_t target_count) : value(std::move(value)), target_count(target_count) {
	}

	Value value;
	idx_t target_count;
};

struct RepeatOperatorData : public GlobalTableFunctionState {
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
	if (inputs[1].IsNull()) {
		throw BinderException("Repeat second parameter cannot be NULL");
	}
	auto repeat_count = inputs[1].GetValue<int64_t>();
	if (repeat_count < 0) {
		throw BinderException("Repeat second parameter cannot be be less than 0");
	}
	return make_uniq<RepeatFunctionData>(inputs[0], NumericCast<idx_t>(repeat_count));
}

static unique_ptr<GlobalTableFunctionState> RepeatInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RepeatOperatorData>();
}

static void RepeatFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RepeatFunctionData>();
	auto &state = data_p.global_state->Cast<RepeatOperatorData>();

	idx_t remaining = MinValue<idx_t>(bind_data.target_count - state.current_count, STANDARD_VECTOR_SIZE);
	output.data[0].Reference(bind_data.value);
	output.SetCardinality(remaining);
	state.current_count += remaining;
}

static unique_ptr<NodeStatistics> RepeatCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<RepeatFunctionData>();
	return make_uniq<NodeStatistics>(bind_data.target_count, bind_data.target_count);
}

void RepeatTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction repeat("repeat", {LogicalType::ANY, LogicalType::BIGINT}, RepeatFunction, RepeatBind, RepeatInit);
	repeat.cardinality = RepeatCardinality;
	set.AddFunction(repeat);
}

} // namespace duckdb
