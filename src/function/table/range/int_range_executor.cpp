#include "duckdb/function/table/range/int_range_executor.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Range (timestamp)
//===--------------------------------------------------------------------===//

void IntRangeExecutor::Reset() {
	RangeExecutor::Reset();
	current_row = 0;
}

void IntRangeExecutor::FetchArguments(DataChunk &input, const vector<unique_ptr<Expression>> &args_list) {

	args_data.Reset();
	// execute the argument expressions
	// execution results (arguments) are kept in args_data chunk
	expr_executor.Execute(input, args_data);

	// verify incoming arguments
	args_data.Verify();
	D_ASSERT(input.size() == args_data.size());
	D_ASSERT(args_data.ColumnCount() == args_list.size());
	D_ASSERT(args_list.size() <= 3 && args_list.size() >= 1);

	first_fetch = false;

	vector<Value> args_values {};

	for (idx_t i = 0; i < args_list.size(); i++) {
		Value v = args_data.GetValue(i, 0);
		if (v.IsNull()) {
			// if any argument is NULL, set the range parameters so that no row is produced
			start = generate_series ? 1 : 0;
			end = 0;
			increment = 1;
			return;
		}
		args_values.push_back(v);
	}

	if (args_list.size() < 3) {
		// no increment is given, set it to default value
		increment = DEFAULT_INCREMENT;
	} else {
		increment = args_values[2].GetValue<int64_t>();
	}
	if (args_list.size() == 1) {
		// if only one argument is given, use it as end and set start to default value
		start = DEFAULT_START;
		end = args_values[0].GetValue<int64_t>();
	} else {
		start = args_values[0].GetValue<int64_t>();
		end = args_values[1].GetValue<int64_t>();
	}
	if (generate_series) { // inclusive upper bound for generate_series
		if (increment < 0) {
			end = end - 1;
		} else {
			end = end + 1;
		}
	}

	if (increment == 0) {
		throw InvalidInputException("increment cannot be 0!");
	}
	if (start > end && increment > 0) {
		throw InvalidInputException(
		    "start is bigger than end, but increment is positive: cannot generate infinite series");
	} else if (start < end && increment < 0) {
		throw InvalidInputException(
		    "start is smaller than end, but increment is negative: cannot generate infinite series");
	}
}

OperatorResultType IntRangeExecutor::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             const vector<unique_ptr<Expression>> &args_list) {

	if (first_fetch) {
		FetchArguments(input, args_list);
	}

	hugeint_t current_value = start + increment * current_row;
	int64_t current_value_i64;
	if (!Hugeint::TryCast<int64_t>(current_value, current_value_i64)) {
		throw InvalidInputException("Range value exceeds the capacity of BIGINT");
	}
	int64_t offset = increment < 0 ? 1 : -1;

	// number of remaining rows to generate
	idx_t remaining = Hugeint::Cast<idx_t>((end - current_value + (increment + offset)) / increment);
	// limit # of generated rows to STANDARD_VECTOR_SIZE
	idx_t num_new_rows = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);

	int64_t increment_i64 = Hugeint::Cast<int64_t>(increment);
	chunk.data[0].Sequence(current_value_i64, increment_i64, num_new_rows);
	chunk.SetCardinality(num_new_rows);
	chunk.Verify();

	if (remaining > num_new_rows) {
		// there are still rows to be generated
		current_row += num_new_rows;
		return OperatorResultType::HAVE_MORE_OUTPUT;
	} else {
		// finished
		Reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}
}

} // namespace duckdb
