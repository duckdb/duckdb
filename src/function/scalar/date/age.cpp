#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
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

static void age_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count == 2 || input.column_count == 1);

	auto &input1 = input.data[0];
	Vector input2;

	if (input.column_count == 1) {
		auto current_timestamp = Timestamp::GetCurrentTimestamp();
		auto value_timestamp = Value::TIMESTAMP(current_timestamp);
		Vector vector_timestamp(value_timestamp);
		input2.Reference(vector_timestamp);
	} else {
		input2.Reference(input.data[1]);
	}
	assert(input1.type == TypeId::INT64);
	assert(input2.type == TypeId::INT64);

	result.SetCount(input1.size());
	result.SetSelVector(input1.sel_vector());

	string output_buffer;
	BinaryExecutor::Execute<timestamp_t, timestamp_t, const char *, true>(
	    input1, input2, result, [&](timestamp_t input1, timestamp_t input2) {
		    return result.AddString(age_scalar_function(input1, input2, output_buffer));
	    });
}

void AgeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet age("age");
	age.AddFunction(ScalarFunction({SQLType::TIMESTAMP}, SQLType::VARCHAR, age_function));
	age.AddFunction(ScalarFunction({SQLType::TIMESTAMP, SQLType::TIMESTAMP}, SQLType::VARCHAR, age_function));
	set.AddFunction(age);
}

} // namespace duckdb
