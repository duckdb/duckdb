#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table/summary.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

struct RangeFunctionBindData : public TableFunctionData {
	int64_t start;
	int64_t end;
	int64_t increment;
};

template <bool GENERATE_SERIES>
static unique_ptr<FunctionData>
RangeFunctionBind(ClientContext &context, vector<Value> &inputs, unordered_map<string, Value> &named_parameters,
                  vector<LogicalType> &input_table_types, vector<string> &input_table_names,
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
		names.emplace_back("generate_series");
	} else {
		names.emplace_back("range");
	}
	return move(result);
}

struct RangeFunctionState : public FunctionOperatorData {
	RangeFunctionState() : current_idx(0) {
	}

	int64_t current_idx;
};

static unique_ptr<FunctionOperatorData> RangeFunctionInit(ClientContext &context, const FunctionData *bind_data,
                                                          const vector<column_t> &column_ids,
                                                          TableFilterCollection *filters) {
	return make_unique<RangeFunctionState>();
}

static void RangeFunction(ClientContext &context, const FunctionData *bind_data_p, FunctionOperatorData *state_p,
                          DataChunk *input, DataChunk &output) {
	auto &bind_data = (RangeFunctionBindData &)*bind_data_p;
	auto &state = (RangeFunctionState &)*state_p;

	auto increment = bind_data.increment;
	auto end = bind_data.end;
	int64_t current_value = bind_data.start + (int64_t)increment * state.current_idx;
	// set the result vector as a sequence vector
	output.data[0].Sequence(current_value, increment);
	idx_t remaining = MinValue<idx_t>((end - current_value) / increment, STANDARD_VECTOR_SIZE);
	// increment the index pointer by the remaining count
	state.current_idx += remaining;
	output.SetCardinality(remaining);
}

unique_ptr<NodeStatistics> RangeCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (RangeFunctionBindData &)*bind_data_p;
	idx_t cardinality = (bind_data.end - bind_data.start) / bind_data.increment;
	return make_unique<NodeStatistics>(cardinality, cardinality);
}

void RangeTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet range("range");

	// single argument range: (end) - implicit start = 0 and increment = 1
	range.AddFunction(TableFunction({LogicalType::BIGINT}, RangeFunction, RangeFunctionBind<false>, RangeFunctionInit,
	                                nullptr, nullptr, nullptr, RangeCardinality));
	// two arguments range: (start, end) - implicit increment = 1
	range.AddFunction(TableFunction({LogicalType::BIGINT, LogicalType::BIGINT}, RangeFunction, RangeFunctionBind<false>,
	                                RangeFunctionInit, nullptr, nullptr, nullptr, RangeCardinality));
	// three arguments range: (start, end, increment)
	range.AddFunction(TableFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT}, RangeFunction,
	                                RangeFunctionBind<false>, RangeFunctionInit, nullptr, nullptr, nullptr,
	                                RangeCardinality));
	set.AddFunction(range);
	// generate_series: similar to range, but inclusive instead of exclusive bounds on the RHS
	TableFunctionSet generate_series("generate_series");
	generate_series.AddFunction(TableFunction({LogicalType::BIGINT}, RangeFunction, RangeFunctionBind<true>,
	                                          RangeFunctionInit, nullptr, nullptr, nullptr, RangeCardinality));
	generate_series.AddFunction(TableFunction({LogicalType::BIGINT, LogicalType::BIGINT}, RangeFunction,
	                                          RangeFunctionBind<true>, RangeFunctionInit, nullptr, nullptr, nullptr,
	                                          RangeCardinality));
	generate_series.AddFunction(TableFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                          RangeFunction, RangeFunctionBind<true>, RangeFunctionInit, nullptr,
	                                          nullptr, nullptr, RangeCardinality));
	set.AddFunction(generate_series);
}

void BuiltinFunctions::RegisterTableFunctions() {
	CheckpointFunction::RegisterFunction(*this);
	GlobTableFunction::RegisterFunction(*this);
	RangeTableFunction::RegisterFunction(*this);
	RepeatTableFunction::RegisterFunction(*this);
	SummaryTableFunction::RegisterFunction(*this);
	UnnestTableFunction::RegisterFunction(*this);
}

} // namespace duckdb
