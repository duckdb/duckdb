#include "duckdb/function/table/range.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

struct RepeatRowFunctionData : public TableFunctionData {
	RepeatRowFunctionData(vector<Value> values, idx_t target_count)
	    : values(std::move(values)), target_count(target_count) {
	}

	const vector<Value> values;
	idx_t target_count;
};

struct RepeatRowOperatorData : public GlobalTableFunctionState {
	RepeatRowOperatorData() : current_count(0) {
	}
	idx_t current_count;
};

static unique_ptr<FunctionData> RepeatRowBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto &inputs = input.inputs;
	for (idx_t input_idx = 0; input_idx < inputs.size(); input_idx++) {
		return_types.push_back(inputs[input_idx].type());
		names.push_back("column" + std::to_string(input_idx));
	}
	auto entry = input.named_parameters.find("num_rows");
	if (entry == input.named_parameters.end()) {
		throw BinderException("repeat_rows requires num_rows to be specified");
	}
	if (inputs.empty()) {
		throw BinderException("repeat_rows requires at least one column to be specified");
	}
	return make_uniq<RepeatRowFunctionData>(inputs, entry->second.GetValue<int64_t>());
}

static unique_ptr<GlobalTableFunctionState> RepeatRowInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RepeatRowOperatorData>();
}

static void RepeatRowFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RepeatRowFunctionData>();
	auto &state = data_p.global_state->Cast<RepeatRowOperatorData>();

	idx_t remaining = MinValue<idx_t>(bind_data.target_count - state.current_count, STANDARD_VECTOR_SIZE);
	for (idx_t val_idx = 0; val_idx < bind_data.values.size(); val_idx++) {
		output.data[val_idx].Reference(bind_data.values[val_idx]);
	}
	output.SetCardinality(remaining);
	state.current_count += remaining;
}

static unique_ptr<NodeStatistics> RepeatRowCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<RepeatRowFunctionData>();
	return make_uniq<NodeStatistics>(bind_data.target_count, bind_data.target_count);
}

void RepeatRowTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction repeat_row("repeat_row", {}, RepeatRowFunction, RepeatRowBind, RepeatRowInit);
	repeat_row.varargs = LogicalType::ANY;
	repeat_row.named_parameters["num_rows"] = LogicalType::BIGINT;
	repeat_row.cardinality = RepeatRowCardinality;
	set.AddFunction(repeat_row);
}

} // namespace duckdb
