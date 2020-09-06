#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/algorithm.hpp"

using namespace std;

namespace duckdb {

struct RangeFunctionBindData : public TableFunctionData {
	int64_t start;
	int64_t end;
	int64_t increment;
};

template<bool GENERATE_SERIES>
static unique_ptr<FunctionData> range_function_bind(ClientContext &context, vector<Value> &inputs,
                                                    unordered_map<string, Value> &named_parameters,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<RangeFunctionBindData>();
	if (inputs.size() < 2) {
		// single argument: only the end is specified
		result->start = 0;
		result->end = inputs[0].GetValue<int64_t>();
	} else {
		// two arguments: first two arguments are start and end
		result->start = inputs[0].GetValue<int64_t>();
		result->end = inputs[1].GetValue<int64_t>();
	}
	if (inputs.size() < 3) {
		result->increment = 1;
	} else {
		result->increment = inputs[2].GetValue<int64_t>();
	}
	if (result->increment == 0) {
		throw BinderException("interval cannot be 0!");
	}
	if (result->start > result->end && result->increment > 0) {
		throw BinderException("start is bigger than end, but increment is positive: cannot generate infinite series");
	} else if (result->start < result->end && result->increment < 0) {
		throw BinderException("start is smaller than end, but increment is negative: cannot generate infinite series");
	}
	return_types.push_back(LogicalType::BIGINT);
	if (GENERATE_SERIES) {
		// generate_series has inclusive bounds on the RHS
		if (result->increment < 0) {
			result->end = result->end - 1;
		} else {
			result->end = result->end + 1;
		}
		names.push_back("generate_series");
	} else {
		names.push_back("range");
	}
	return move(result);
}

struct RangeFunctionState : public FunctionOperatorData {
	RangeFunctionState() : current_idx(0) {}

	int64_t current_idx;
};

static unique_ptr<FunctionOperatorData> range_function_init(ClientContext &context, const FunctionData *bind_data, OperatorTaskInfo *task_info,
	vector<column_t> &column_ids, unordered_map<idx_t, vector<TableFilter>> &table_filters) {
	return make_unique<RangeFunctionState>();
}

static void range_function(ClientContext &context, const FunctionData *bind_data_, FunctionOperatorData *state_, DataChunk &output) {
	auto &bind_data = (RangeFunctionBindData &)*bind_data_;
	auto &state = (RangeFunctionState &)*state_;

	auto increment = bind_data.increment;
	auto end = bind_data.end;
	int64_t current_value = bind_data.start + (int64_t)increment * state.current_idx;
	// set the result vector as a sequence vector
	output.data[0].Sequence(current_value, increment);
	idx_t remaining = min<idx_t>((end - current_value) / increment, STANDARD_VECTOR_SIZE);
	// increment the index pointer by the remaining count
	state.current_idx += remaining;
	output.SetCardinality(remaining);
}

idx_t range_cardinality(const FunctionData *bind_data_) {
	auto &bind_data = (RangeFunctionBindData &)*bind_data_;
	return (bind_data.end - bind_data.start) / bind_data.increment;
}

void RangeTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet range("range");

	// single argument range: (end) - implicit start = 0 and increment = 1
	range.AddFunction(TableFunction({LogicalType::BIGINT}, range_function, range_function_bind<false>, range_function_init, nullptr, nullptr, nullptr, range_cardinality));
	// two arguments range: (start, end) - implicit increment = 1
	range.AddFunction(TableFunction({LogicalType::BIGINT, LogicalType::BIGINT}, range_function, range_function_bind<false>, range_function_init, nullptr, nullptr, nullptr, range_cardinality));
	// three arguments range: (start, end, increment)
	range.AddFunction(TableFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT}, range_function, range_function_bind<false>, range_function_init, nullptr, nullptr, nullptr, range_cardinality));
	set.AddFunction(range);
	// generate_series: similar to range, but inclusive instead of exclusive bounds on the RHS
	TableFunctionSet generate_series("generate_series");
	generate_series.AddFunction(TableFunction({LogicalType::BIGINT}, range_function, range_function_bind<true>, range_function_init, nullptr, nullptr, nullptr, range_cardinality));
	generate_series.AddFunction(TableFunction({LogicalType::BIGINT, LogicalType::BIGINT}, range_function, range_function_bind<true>, range_function_init, nullptr, nullptr, nullptr, range_cardinality));
	generate_series.AddFunction(TableFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT}, range_function, range_function_bind<true>, range_function_init, nullptr, nullptr, nullptr, range_cardinality));
	set.AddFunction(generate_series);
}

void BuiltinFunctions::RegisterTableFunctions() {
	RangeTableFunction::RegisterFunction(*this);
	RepeatTableFunction::RegisterFunction(*this);
}

} // namespace duckdb
