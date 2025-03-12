#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table/summary.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Range (integers)
//===--------------------------------------------------------------------===//
static void GetParameters(int64_t values[], idx_t value_count, hugeint_t &start, hugeint_t &end, hugeint_t &increment) {
	if (value_count < 2) {
		// single argument: only the end is specified
		start = 0;
		end = values[0];
	} else {
		// two arguments: first two arguments are start and end
		start = values[0];
		end = values[1];
	}
	if (value_count < 3) {
		increment = 1;
	} else {
		increment = values[2];
	}
}

struct RangeFunctionBindData : public TableFunctionData {
	explicit RangeFunctionBindData(const vector<Value> &inputs) : cardinality(0) {
		int64_t values[3];
		for (idx_t i = 0; i < inputs.size(); i++) {
			if (inputs[i].IsNull()) {
				return;
			}
			values[i] = inputs[i].GetValue<int64_t>();
		}
		hugeint_t start;
		hugeint_t end;
		hugeint_t increment;
		GetParameters(values, inputs.size(), start, end, increment);
		cardinality = Hugeint::Cast<idx_t>((end - start) / increment);
	}

	idx_t cardinality;
};

template <bool GENERATE_SERIES>
static unique_ptr<FunctionData> RangeFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BIGINT);
	if (GENERATE_SERIES) {
		names.emplace_back("generate_series");
	} else {
		names.emplace_back("range");
	}
	if (input.inputs.empty() || input.inputs.size() > 3) {
		return nullptr;
	}
	return make_uniq<RangeFunctionBindData>(input.inputs);
}

struct RangeFunctionLocalState : public LocalTableFunctionState {
	RangeFunctionLocalState() {
	}

	bool initialized_row = false;
	idx_t current_input_row = 0;
	idx_t current_idx = 0;

	hugeint_t start;
	hugeint_t end;
	hugeint_t increment;

	bool empty_range = false;
};

static unique_ptr<LocalTableFunctionState> RangeFunctionLocalInit(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
	return make_uniq<RangeFunctionLocalState>();
}

template <bool GENERATE_SERIES>
static void GenerateRangeParameters(DataChunk &input, idx_t row_id, RangeFunctionLocalState &result) {
	input.Flatten();
	for (idx_t c = 0; c < input.ColumnCount(); c++) {
		if (FlatVector::IsNull(input.data[c], row_id)) {
			result.start = GENERATE_SERIES ? 1 : 0;
			result.end = 0;
			result.increment = 1;
			return;
		}
	}
	int64_t values[3];
	for (idx_t c = 0; c < input.ColumnCount(); c++) {
		if (c >= 3) {
			throw InternalException("Unsupported parameter count for range function");
		}
		values[c] = FlatVector::GetValue<int64_t>(input.data[c], row_id);
	}
	GetParameters(values, input.ColumnCount(), result.start, result.end, result.increment);
	if (result.increment == 0) {
		throw BinderException("interval cannot be 0!");
	}
	if (result.start > result.end && result.increment > 0) {
		result.empty_range = true;
	}
	if (result.start < result.end && result.increment < 0) {
		result.empty_range = true;
	}
	if (GENERATE_SERIES) {
		// generate_series has inclusive bounds on the RHS
		if (result.increment < 0) {
			result.end = result.end - 1;
		} else {
			result.end = result.end + 1;
		}
	}
}

template <bool GENERATE_SERIES>
static OperatorResultType RangeFunction(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                        DataChunk &output) {
	auto &state = data_p.local_state->Cast<RangeFunctionLocalState>();
	while (true) {
		if (!state.initialized_row) {
			// initialize for the current input row
			if (state.current_input_row >= input.size()) {
				// ran out of rows
				state.current_input_row = 0;
				state.initialized_row = false;
				return OperatorResultType::NEED_MORE_INPUT;
			}
			GenerateRangeParameters<GENERATE_SERIES>(input, state.current_input_row, state);
			state.initialized_row = true;
			state.current_idx = 0;
		}
		if (state.empty_range) {
			// empty range
			output.SetCardinality(0);
			state.current_input_row++;
			state.initialized_row = false;
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		auto increment = state.increment;
		auto end = state.end;
		hugeint_t current_value = state.start + increment * UnsafeNumericCast<int64_t>(state.current_idx);
		int64_t current_value_i64;
		if (!Hugeint::TryCast<int64_t>(current_value, current_value_i64)) {
			// move to next row
			state.current_input_row++;
			state.initialized_row = false;
			continue;
		}
		int64_t offset = increment < 0 ? 1 : -1;
		idx_t remaining = MinValue<idx_t>(
		    Hugeint::Cast<idx_t>((end - current_value + (increment + offset)) / increment), STANDARD_VECTOR_SIZE);
		// set the result vector as a sequence vector
		output.data[0].Sequence(current_value_i64, Hugeint::Cast<int64_t>(increment), remaining);
		// increment the index pointer by the remaining count
		state.current_idx += remaining;
		output.SetCardinality(remaining);
		if (remaining == 0) {
			// move to next row
			state.current_input_row++;
			state.initialized_row = false;
			continue;
		}
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
}

unique_ptr<NodeStatistics> RangeCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	if (!bind_data_p) {
		return nullptr;
	}
	auto &bind_data = bind_data_p->Cast<RangeFunctionBindData>();
	return make_uniq<NodeStatistics>(bind_data.cardinality, bind_data.cardinality);
}

//===--------------------------------------------------------------------===//
// Range (timestamp)
//===--------------------------------------------------------------------===//
template <bool GENERATE_SERIES>
static unique_ptr<FunctionData> RangeDateTimeBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::TIMESTAMP);
	if (GENERATE_SERIES) {
		names.emplace_back("generate_series");
	} else {
		names.emplace_back("range");
	}
	return nullptr;
}

struct RangeDateTimeLocalState : public LocalTableFunctionState {
	RangeDateTimeLocalState() {
	}

	bool initialized_row = false;
	idx_t current_input_row = 0;
	timestamp_t current_state;

	timestamp_t start;
	timestamp_t end;
	interval_t increment;
	bool inclusive_bound;
	bool greater_than_check;

	bool empty_range = false;

	bool Finished(timestamp_t current_value) const {
		if (greater_than_check) {
			if (inclusive_bound) {
				return current_value > end;
			} else {
				return current_value >= end;
			}
		} else {
			if (inclusive_bound) {
				return current_value < end;
			} else {
				return current_value <= end;
			}
		}
	}
};

template <bool GENERATE_SERIES>
static void GenerateRangeDateTimeParameters(DataChunk &input, idx_t row_id, RangeDateTimeLocalState &result) {
	input.Flatten();

	for (idx_t c = 0; c < input.ColumnCount(); c++) {
		if (FlatVector::IsNull(input.data[c], row_id)) {
			result.start = timestamp_t(0);
			result.end = timestamp_t(0);
			result.increment = interval_t();
			result.greater_than_check = true;
			result.inclusive_bound = false;
			return;
		}
	}

	result.start = FlatVector::GetValue<timestamp_t>(input.data[0], row_id);
	result.end = FlatVector::GetValue<timestamp_t>(input.data[1], row_id);
	result.increment = FlatVector::GetValue<interval_t>(input.data[2], row_id);

	// Infinities either cause errors or infinite loops, so just ban them
	if (!Timestamp::IsFinite(result.start) || !Timestamp::IsFinite(result.end)) {
		throw BinderException("RANGE with infinite bounds is not supported");
	}

	if (result.increment.months == 0 && result.increment.days == 0 && result.increment.micros == 0) {
		throw BinderException("interval cannot be 0!");
	}
	// all elements should point in the same direction
	if (result.increment.months > 0 || result.increment.days > 0 || result.increment.micros > 0) {
		if (result.increment.months < 0 || result.increment.days < 0 || result.increment.micros < 0) {
			throw BinderException("RANGE with composite interval that has mixed signs is not supported");
		}
		result.greater_than_check = true;
		if (result.start > result.end) {
			result.empty_range = true;
		}
	} else {
		result.greater_than_check = false;
		if (result.start < result.end) {
			result.empty_range = true;
		}
	}
	result.inclusive_bound = GENERATE_SERIES;
}

static unique_ptr<LocalTableFunctionState> RangeDateTimeLocalInit(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
	return make_uniq<RangeDateTimeLocalState>();
}

template <bool GENERATE_SERIES>
static OperatorResultType RangeDateTimeFunction(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                                DataChunk &output) {
	auto &state = data_p.local_state->Cast<RangeDateTimeLocalState>();
	while (true) {
		if (!state.initialized_row) {
			// initialize for the current input row
			if (state.current_input_row >= input.size()) {
				// ran out of rows
				state.current_input_row = 0;
				state.initialized_row = false;
				return OperatorResultType::NEED_MORE_INPUT;
			}
			GenerateRangeDateTimeParameters<GENERATE_SERIES>(input, state.current_input_row, state);
			state.initialized_row = true;
			state.current_state = state.start;
		}
		if (state.empty_range) {
			// empty range
			output.SetCardinality(0);
			state.current_input_row++;
			state.initialized_row = false;
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		idx_t size = 0;
		auto data = FlatVector::GetData<timestamp_t>(output.data[0]);
		while (true) {
			if (state.Finished(state.current_state)) {
				break;
			}
			if (size >= STANDARD_VECTOR_SIZE) {
				break;
			}
			data[size++] = state.current_state;
			state.current_state =
			    AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(state.current_state, state.increment);
		}
		if (size == 0) {
			// move to next row
			state.current_input_row++;
			state.initialized_row = false;
			continue;
		}
		output.SetCardinality(size);
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
}

void RangeTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet range("range");

	TableFunction range_function({LogicalType::BIGINT}, nullptr, RangeFunctionBind<false>, nullptr,
	                             RangeFunctionLocalInit);
	range_function.in_out_function = RangeFunction<false>;
	range_function.cardinality = RangeCardinality;

	// single argument range: (end) - implicit start = 0 and increment = 1
	range.AddFunction(range_function);
	// two arguments range: (start, end) - implicit increment = 1
	range_function.arguments = {LogicalType::BIGINT, LogicalType::BIGINT};
	range.AddFunction(range_function);
	// three arguments range: (start, end, increment)
	range_function.arguments = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	range.AddFunction(range_function);
	TableFunction range_in_out({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::INTERVAL}, nullptr,
	                           RangeDateTimeBind<false>, nullptr, RangeDateTimeLocalInit);
	range_in_out.in_out_function = RangeDateTimeFunction<false>;
	range.AddFunction(range_in_out);
	set.AddFunction(range);
	// generate_series: similar to range, but inclusive instead of exclusive bounds on the RHS
	TableFunctionSet generate_series("generate_series");
	range_function.bind = RangeFunctionBind<true>;
	range_function.in_out_function = RangeFunction<true>;
	range_function.arguments = {LogicalType::BIGINT};
	generate_series.AddFunction(range_function);
	range_function.arguments = {LogicalType::BIGINT, LogicalType::BIGINT};
	generate_series.AddFunction(range_function);
	range_function.arguments = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	generate_series.AddFunction(range_function);
	TableFunction generate_series_in_out({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::INTERVAL},
	                                     nullptr, RangeDateTimeBind<true>, nullptr, RangeDateTimeLocalInit);
	generate_series_in_out.in_out_function = RangeDateTimeFunction<true>;
	generate_series.AddFunction(generate_series_in_out);
	set.AddFunction(generate_series);
}

void BuiltinFunctions::RegisterTableFunctions() {
	CheckpointFunction::RegisterFunction(*this);
	GlobTableFunction::RegisterFunction(*this);
	RangeTableFunction::RegisterFunction(*this);
	RepeatTableFunction::RegisterFunction(*this);
	SummaryTableFunction::RegisterFunction(*this);
	UnnestTableFunction::RegisterFunction(*this);
	RepeatRowTableFunction::RegisterFunction(*this);
	CSVSnifferFunction::RegisterFunction(*this);
	ReadBlobFunction::RegisterFunction(*this);
	ReadTextFunction::RegisterFunction(*this);
	QueryTableFunction::RegisterFunction(*this);
}

} // namespace duckdb
