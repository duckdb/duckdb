#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/algorithm.hpp"

using namespace std;

namespace duckdb {

struct RangeFunctionData : public TableFunctionData {
	Value start;
	Value end;
	Value increment;
	idx_t current_idx;
};

static unique_ptr<FunctionData> range_function_bind(ClientContext &context, vector<Value> &inputs,
                                                    unordered_map<string, Value> &named_parameters,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<RangeFunctionData>();
	if (inputs.size() < 2) {
		// single argument: only the end is specified
		result->start = Value::BIGINT(0);
		result->end = inputs[0].CastAs(LogicalType::BIGINT);
	} else {
		// two arguments: first two arguments are start and end
		result->start = inputs[0].CastAs(LogicalType::BIGINT);
		result->end = inputs[1].CastAs(LogicalType::BIGINT);
	}
	if (inputs.size() < 3) {
		result->increment = Value::BIGINT(1);
	} else {
		result->increment = inputs[2].CastAs(LogicalType::BIGINT);
	}
	if (result->increment == 0) {
		throw BinderException("interval cannot be 0!");
	}
	if (result->start > result->end && result->increment > 0) {
		throw BinderException("start is bigger than end, but increment is positive: cannot generate infinite series");
	} else if (result->start < result->end && result->increment < 0) {
		throw BinderException("start is smaller than end, but increment is negative: cannot generate infinite series");
	}
	result->current_idx = 0;
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("range");
	return move(result);
}

static void range_function(ClientContext &context, vector<Value> &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = ((RangeFunctionData &)*dataptr);
	auto increment = data.increment.value_.bigint;
	auto end = data.end.value_.bigint;
	int64_t current_value = data.start.value_.bigint + (int64_t)increment * data.current_idx;
	// set the result vector as a sequence vector
	output.data[0].Sequence(current_value, increment);
	idx_t remaining = min<int64_t>((end - current_value) / increment, STANDARD_VECTOR_SIZE);
	// increment the index pointer by the remaining count
	data.current_idx += remaining;
	output.SetCardinality(remaining);
}

void RangeTableFunction::RegisterFunction(BuiltinFunctions &set) {
	// TableFunctionSet range("range");

	// // FIXME
	// // // single argument range: (end) - implicit start = 0 and increment = 1
	// // range.AddFunction(TableFunction({LogicalType::BIGINT}, range_function_bind, range_function));
	// // // two arguments range: (start, end) - implicit increment = 1
	// // range.AddFunction(TableFunction({LogicalType::BIGINT, LogicalType::BIGINT}, range_function_bind, range_function));
	// // // three arguments range: (start, end, increment)
	// // range.AddFunction(TableFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	// //                                 range_function_bind, range_function));
	// set.AddFunction(range);
	// range.name = "generate_series";
	// set.AddFunction(range);
}

void BuiltinFunctions::RegisterTableFunctions() {
	RangeTableFunction::RegisterFunction(*this);
	RepeatTableFunction::RegisterFunction(*this);
}

} // namespace duckdb
