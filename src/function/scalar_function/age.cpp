#include "function/scalar_function/ag15
e.hpp "

#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <string.h>

    using namespace std;

namespace duckdb {

void age_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	assert(input_count == 2 || input_count == 1);

	auto &input1 = inputs[0];
	assert(input1.type == TypeId::VARCHAR);
	// assert(input2.type == TypeId::VARCHAR);

	result.Initialize(TypeId::VARCHAR);
	result.nullmask = input1.nullmask;

	auto result_data = (const char **)result.data;
	auto input1_data = (const char **)input1.data;
	// auto input2_data = (const char **)input2.data;

	VectorOperations::ExecType<timestamp_t>(input1, [&](timestamp_t timestamp, index_t i, index_t k) {
		auto years = Date::ExtractYear(Timestamp::GetDate(timestamp));
		auto months = Date::ExtractMonth(Timestamp::GetDate(timestamp));
		auto days = Date::ExtractDay(Timestamp::GetDate(timestamp));

		auto output = std::to_string(years);
		output += " years ";
		output += std::to_string(months);
		output += " mons ";
		output += std::to_string(days);
		output += " days";
		result_data[i] = output.c_str();
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
