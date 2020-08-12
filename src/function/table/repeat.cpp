#include "duckdb/function/table/range.hpp"
#include "duckdb/common/algorithm.hpp"

using namespace std;

namespace duckdb {

struct RepeatFunctionData : public TableFunctionData {
	RepeatFunctionData(idx_t target_count) : current_count(0), target_count(target_count) { }

	idx_t current_count;
	idx_t target_count;
};

static unique_ptr<FunctionData> repeat_bind(ClientContext &context, vector<Value> &inputs, unordered_map<string, Value> &named_parameters,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	// the repeat function returns the type of the first argument
	return_types.push_back(inputs[0].GetLogicalType());
	names.push_back(inputs[0].ToString());
	return make_unique<RepeatFunctionData>(inputs[1].GetValue<int64_t>());
}

static void repeat_function(ClientContext &context, vector<Value> &input, DataChunk &output, FunctionData *dataptr) {
	auto &repeat = (RepeatFunctionData &) *dataptr;
	idx_t remaining = min<idx_t>(repeat.target_count - repeat.current_count, STANDARD_VECTOR_SIZE);
	output.data[0].Reference(input[0]);
	output.SetCardinality(remaining);
	repeat.current_count += remaining;
}

void RepeatTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction repeat("repeat", {LogicalType::ANY, LogicalType::BIGINT}, repeat_bind, repeat_function, nullptr);
	set.AddFunction(repeat);
}

} // namespace duckdb
