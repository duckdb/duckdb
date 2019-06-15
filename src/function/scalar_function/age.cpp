#include "function/scalar_function/age.hpp"

using namespace std;

namespace duckdb {

void age_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	assert(input_count == 2 || input_count == 1);

	auto &input1 = inputs[0];
	Vector input2;

	if (input_count == 1) {
		auto current_timestamp = Timestamp::GetCurrentTimestamp();
		auto value_timestamp = Value::TIMESTAMP(current_timestamp);
		Vector vector_timestamp(value_timestamp);
		vector_timestamp.Move(input2);
	} else {
		inputs[1].Move(input2);
	}
	assert(input1.type == TypeId::BIGINT);
	assert(input2.type == TypeId::BIGINT);

	result.Initialize(TypeId::VARCHAR);
	result.nullmask = input1.nullmask | input2.nullmask;
	result.count = input1.count;
	result.sel_vector = input1.sel_vector;

	auto result_data = (const char **)result.data;
	auto input1_data = (timestamp_t *)input1.data;
	auto input2_data = (timestamp_t *)input2.data;

	VectorOperations::BinaryExec(input1, input2, result,
	                             [&](index_t input1_index, index_t input2_index, index_t result_index) {
		                             // One of them is NULL
		                             if (result.nullmask[result_index]) {
			                             return;
		                             }
		                             auto input1 = input1_data[input1_index];
		                             auto input2 = input2_data[input2_index];

		                             auto interval = Timestamp::GetDifference(input1, input2);
		                             auto timestamp = Timestamp::IntervalToTimestamp(interval);
		                             auto years = timestamp.year;
		                             auto months = timestamp.month;
		                             auto days = timestamp.day;
		                             auto time = interval.time;

		                             std::string output{""};
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

		                             result_data[result_index] = result.string_heap.AddString(output.c_str());
	                             });
}

// TODO: extend to support arbitrary number of arguments, not only two
bool age_matches_arguments(vector<SQLType> &arguments) {
	bool arguments_number = arguments.size() == 1 || arguments.size() == 2;
	bool arguments_type = false;
	for (auto argument : arguments) {
		arguments_type = argument.id == SQLTypeId::TIMESTAMP;
	}
	return arguments_number && arguments_type;
}

SQLType age_get_return_type(vector<SQLType> &arguments) {
	return SQLType(SQLTypeId::VARCHAR);
}

} // namespace duckdb
