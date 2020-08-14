#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

using namespace std;

namespace duckdb {

static void age_function_standard(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == 1);
	auto current_timestamp = Timestamp::GetCurrentTimestamp();

	UnaryExecutor::Execute<timestamp_t, interval_t, true>(input.data[0], result, input.size(), [&](timestamp_t input) {
		return Interval::GetDifference(current_timestamp, input);
	});
}

static void age_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == 2);

	BinaryExecutor::Execute<timestamp_t, timestamp_t, interval_t, true>(
	    input.data[0], input.data[1], result, input.size(),
	    [&](timestamp_t input1, timestamp_t input2) { return Interval::GetDifference(input1, input2); });
}

void AgeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet age("age");
	age.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::INTERVAL, age_function_standard));
	age.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP}, LogicalType::INTERVAL, age_function));
	set.AddFunction(age);
}

} // namespace duckdb
