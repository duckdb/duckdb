#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table/summary.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/operator/add.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Range (integers)
//===--------------------------------------------------------------------===//
struct RangeFunctionBindData : public TableFunctionData {
	hugeint_t start;
	hugeint_t end;
	hugeint_t increment;

public:
	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const RangeFunctionBindData &)other_p;
		return other.start == start && other.end == end && other.increment == increment;
	}
};

template <bool GENERATE_SERIES>
static unique_ptr<FunctionData> RangeFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<RangeFunctionBindData>();
	auto &inputs = input.inputs;
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
	return_types.emplace_back(LogicalType::BIGINT);
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
                          DataChunk &output) {
	auto &bind_data = (RangeFunctionBindData &)*bind_data_p;
	auto &state = (RangeFunctionState &)*state_p;

	auto increment = bind_data.increment;
	auto end = bind_data.end;
	hugeint_t current_value = bind_data.start + increment * state.current_idx;
	int64_t current_value_i64;
	if (!Hugeint::TryCast<int64_t>(current_value, current_value_i64)) {
		return;
	}
	// set the result vector as a sequence vector
	output.data[0].Sequence(current_value_i64, Hugeint::Cast<int64_t>(increment));
	int64_t offset = increment < 0 ? 1 : -1;
	idx_t remaining = MinValue<idx_t>(Hugeint::Cast<idx_t>((end - current_value + (increment + offset)) / increment),
	                                  STANDARD_VECTOR_SIZE);
	// increment the index pointer by the remaining count
	state.current_idx += remaining;
	output.SetCardinality(remaining);
}

unique_ptr<NodeStatistics> RangeCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (RangeFunctionBindData &)*bind_data_p;
	idx_t cardinality = Hugeint::Cast<idx_t>((bind_data.end - bind_data.start) / bind_data.increment);
	return make_unique<NodeStatistics>(cardinality, cardinality);
}

//===--------------------------------------------------------------------===//
// Range (timestamp)
//===--------------------------------------------------------------------===//
struct RangeDateTimeBindData : public TableFunctionData {
	timestamp_t start;
	timestamp_t end;
	interval_t increment;
	bool inclusive_bound;
	bool greater_than_check;

public:
	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const RangeDateTimeBindData &)other_p;
		return other.start == start && other.end == end && other.increment == increment &&
		       other.inclusive_bound == inclusive_bound && other.greater_than_check == greater_than_check;
	}

	bool Finished(timestamp_t current_value) {
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
static unique_ptr<FunctionData> RangeDateTimeBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<RangeDateTimeBindData>();
	auto &inputs = input.inputs;
	D_ASSERT(inputs.size() == 3);
	result->start = inputs[0].GetValue<timestamp_t>();
	result->end = inputs[1].GetValue<timestamp_t>();
	result->increment = inputs[2].GetValue<interval_t>();

	if (result->increment.months == 0 && result->increment.days == 0 && result->increment.micros == 0) {
		throw BinderException("interval cannot be 0!");
	}
	// all elements should point in the same direction
	if (result->increment.months > 0 || result->increment.days > 0 || result->increment.micros > 0) {
		if (result->increment.months < 0 || result->increment.days < 0 || result->increment.micros < 0) {
			throw BinderException("RANGE with composite interval that has mixed signs is not supported");
		}
		result->greater_than_check = true;
		if (result->start > result->end) {
			throw BinderException(
			    "start is bigger than end, but increment is positive: cannot generate infinite series");
		}
	} else {
		result->greater_than_check = false;
		if (result->start < result->end) {
			throw BinderException(
			    "start is smaller than end, but increment is negative: cannot generate infinite series");
		}
	}
	return_types.push_back(inputs[0].type());
	if (GENERATE_SERIES) {
		// generate_series has inclusive bounds on the RHS
		result->inclusive_bound = true;
		names.emplace_back("generate_series");
	} else {
		result->inclusive_bound = false;
		names.emplace_back("range");
	}
	return move(result);
}

struct RangeDateTimeState : public FunctionOperatorData {
	explicit RangeDateTimeState(timestamp_t start_p) : current_state(start_p) {
	}

	timestamp_t current_state;
	bool finished = false;
};

static unique_ptr<FunctionOperatorData> RangeDateTimeInit(ClientContext &context, const FunctionData *bind_data_p,
                                                          const vector<column_t> &column_ids,
                                                          TableFilterCollection *filters) {
	auto &bind_data = (RangeDateTimeBindData &)*bind_data_p;
	return make_unique<RangeDateTimeState>(bind_data.start);
}

static void RangeDateTimeFunction(ClientContext &context, const FunctionData *bind_data_p,
                                  FunctionOperatorData *state_p, DataChunk &output) {
	auto &bind_data = (RangeDateTimeBindData &)*bind_data_p;
	auto &state = (RangeDateTimeState &)*state_p;
	if (state.finished) {
		return;
	}

	idx_t size = 0;
	auto data = FlatVector::GetData<timestamp_t>(output.data[0]);
	while (true) {
		data[size++] = state.current_state;
		state.current_state =
		    AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(state.current_state, bind_data.increment);
		if (bind_data.Finished(state.current_state)) {
			state.finished = true;
			break;
		}
		if (size >= STANDARD_VECTOR_SIZE) {
			break;
		}
	}
	output.SetCardinality(size);
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
	range.AddFunction(TableFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::INTERVAL},
	                                RangeDateTimeFunction, RangeDateTimeBind<false>, RangeDateTimeInit));
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
	generate_series.AddFunction(TableFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::INTERVAL},
	                                          RangeDateTimeFunction, RangeDateTimeBind<true>, RangeDateTimeInit));
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
