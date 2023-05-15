#include "duckdb/execution/operator/projection/physical_time_range.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Range (timestamp)
//===--------------------------------------------------------------------===//

class TimeRangeOperatorState : public OperatorState {
public:
	TimeRangeOperatorState(ClientContext &context, const vector<unique_ptr<Expression>> &args_list,
	                       bool generate_series_p)
	    : first_fetch(true), generate_series(generate_series_p), executor(context) {

		// we add each expression in the args_list to the expression executor
		vector<LogicalType> args_data_types;
		for (auto &exp : args_list) {
			D_ASSERT(exp->type == ExpressionType::BOUND_REF);
			auto &ref = exp->Cast<BoundReferenceExpression>();
			args_data_types.push_back(exp->return_type);
			executor.AddExpression(ref);
		}

		auto &allocator = Allocator::Get(context);
		args_data.Initialize(allocator, args_data_types);
	}

	//! load arguments from args_list, using the data in the input chunk
	void FetchArguments(DataChunk &input, const vector<unique_ptr<Expression>> &args_list);

	bool first_fetch;

	// indicate whether generate_series is being executed (true) or range (false)
	bool generate_series;

	timestamp_t start;
	timestamp_t end;
	interval_t increment;
	bool positive_increment;

	timestamp_t current_timestamp;
	ExpressionExecutor executor;
	DataChunk args_data;

public:
	//! Reset the fields of the range operator state
	void Reset();
	bool Finished(timestamp_t current_value);
};

void TimeRangeOperatorState::Reset() {
	first_fetch = true;
}

void TimeRangeOperatorState::FetchArguments(DataChunk &input, const vector<unique_ptr<Expression>> &args_list) {

	args_data.Reset();
	// execute the argument expressions
	// execution results (arguments) are kept in state.args_data chunk
	executor.Execute(input, args_data);

	// verify incoming arguments
	args_data.Verify();
	D_ASSERT(input.size() == args_data.size());
	D_ASSERT(args_data.ColumnCount() == args_list.size());
	D_ASSERT(args_list.size() == 3);

	first_fetch = false;

	start = args_data.GetValue(0, 0).GetValue<timestamp_t>();
	end = args_data.GetValue(1, 0).GetValue<timestamp_t>();
	increment = args_data.GetValue(2, 0).GetValue<interval_t>();

	current_timestamp = start;

	// Infinities either cause errors or infinite loops, so just ban them
	if (!Timestamp::IsFinite(start) || !Timestamp::IsFinite(end)) {
		throw InvalidInputException("RANGE with infinite bounds is not supported");
	}

	if (increment.months == 0 && increment.days == 0 && increment.micros == 0) {
		throw InvalidInputException("interval cannot be 0!");
	}
	// all elements should point in the same direction
	if (increment.months > 0 || increment.days > 0 || increment.micros > 0) {
		if (increment.months < 0 || increment.days < 0 || increment.micros < 0) {
			throw InvalidInputException("RANGE with composite interval that has mixed signs is not supported");
		}
		positive_increment = true;
		if (start > end) {
			throw InvalidInputException(
			    "start is bigger than end, but increment is positive: cannot generate infinite series");
		}
	} else {
		positive_increment = false;
		if (start < end) {
			throw InvalidInputException(
			    "start is smaller than end, but increment is negative: cannot generate infinite series");
		}
	}
}

bool TimeRangeOperatorState::Finished(timestamp_t current_value) {
	if (positive_increment) {
		if (generate_series) {
			return current_value > end;
		} else {
			return current_value >= end;
		}
	} else {
		if (generate_series) {
			return current_value < end;
		} else {
			return current_value <= end;
		}
	}
}

unique_ptr<OperatorState> PhysicalTimeRange::GetOperatorState(ExecutionContext &context) const {
	return PhysicalTimeRange::GetState(context, args_list, generate_series);
}

unique_ptr<OperatorState> PhysicalTimeRange::GetState(ExecutionContext &context,
                                                      const vector<unique_ptr<Expression>> &args_list,
                                                      bool generate_series) {
	return make_uniq<TimeRangeOperatorState>(context.client, args_list, generate_series);
}

OperatorResultType PhysicalTimeRange::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                      OperatorState &state_p,
                                                      const vector<unique_ptr<Expression>> &args_list) {

	auto &state = state_p.Cast<TimeRangeOperatorState>();

	if (state.first_fetch) {
		state.FetchArguments(input, args_list);
	}

	OperatorResultType result;
	idx_t num_new_rows = 0;
	auto data = FlatVector::GetData<timestamp_t>(chunk.data[0]);
	while (true) {
		data[num_new_rows++] = state.current_timestamp;
		state.current_timestamp =
		    AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(state.current_timestamp, state.increment);
		if (state.Finished(state.current_timestamp)) {
			state.Reset();
			result = OperatorResultType::NEED_MORE_INPUT;
			break;
		}
		if (num_new_rows >= STANDARD_VECTOR_SIZE) {
			result = OperatorResultType::HAVE_MORE_OUTPUT;
			break;
		}
	}
	chunk.SetCardinality(num_new_rows);
	chunk.Verify();
	return result;
}

OperatorResultType PhysicalTimeRange::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                              GlobalOperatorState &, OperatorState &state) const {
	return ExecuteInternal(context, input, chunk, state, args_list);
}

} // namespace duckdb
