#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

using namespace std;

namespace duckdb {

static const char *age_scalar_function(timestamp_t input1, timestamp_t input2, string &output) {
	auto interval = Timestamp::GetDifference(input1, input2);
	auto timestamp = Timestamp::IntervalToTimestamp(interval);
	auto years = timestamp.year;
	auto months = timestamp.month;
	auto days = timestamp.day;
	auto time = interval.time;

	output = "";
	if (years == 0 && months == 0 && days == 0) {
		output += "00:00:00";
	} else {
		if (years != 0) {
			output = std::to_string(years);
			output += " years ";
		}
		if (months != 0) {
			output += std::to_string(months);
			output += " mons ";
		}
		if (days != 0) {
			output += std::to_string(days);
			output += " days";
		}
		if (time != 0) {
			output += " ";
			output += Time::ToString(time);
		}
	}
	return output.c_str();
}

static void age_function_standard(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == 1);
	auto current_timestamp = Timestamp::GetCurrentTimestamp();

	string output_buffer;
	UnaryExecutor::Execute<timestamp_t, string_t, true>(input.data[0], result, input.size(), [&](timestamp_t input) {
		return StringVector::AddString(result, age_scalar_function(input, current_timestamp, output_buffer));
	});
}

static void age_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == 2);

	string output_buffer;
	BinaryExecutor::Execute<timestamp_t, timestamp_t, string_t, true>(
	    input.data[0], input.data[1], result, input.size(), [&](timestamp_t input1, timestamp_t input2) {
		    return StringVector::AddString(result, age_scalar_function(input1, input2, output_buffer));
	    });
}

void AgeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet age("age");
	age.AddFunction(ScalarFunction({SQLType::TIMESTAMP}, SQLType::VARCHAR, age_function_standard));
	age.AddFunction(ScalarFunction({SQLType::TIMESTAMP, SQLType::TIMESTAMP}, SQLType::VARCHAR, age_function));
	set.AddFunction(age);
}

} // namespace duckdb
