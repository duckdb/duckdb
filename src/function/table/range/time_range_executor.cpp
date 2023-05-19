#include "duckdb/function/table/range/time_range_executor.hpp"

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

void TimeRangeExecutor::FetchArguments(DataChunk &input, const vector<unique_ptr<Expression>> &args_list) {

	args_data.Reset();
	// execute the argument expressions
	// execution results (arguments) are kept in args_data chunk
	expr_executor.Execute(input, args_data);

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

bool TimeRangeExecutor::Finished(timestamp_t current_value) {
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

void TimeRangeExecutor::StepForward() {
	current_timestamp = AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(current_timestamp, increment);
}

OperatorResultType TimeRangeExecutor::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                              const vector<unique_ptr<Expression>> &args_list) {

	if (first_fetch) {
		FetchArguments(input, args_list);
	}

	OperatorResultType result;
	idx_t num_new_rows = 0;
	auto data = FlatVector::GetData<timestamp_t>(chunk.data[0]);
	while (true) {
		data[num_new_rows++] = current_timestamp;
		StepForward();
		if (Finished(current_timestamp)) {
			Reset();
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

} // namespace duckdb
