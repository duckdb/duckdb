#include "function/scalar_function/year.hpp"

#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/types/timestamp.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {
namespace function {

void year_function(Vector inputs[], size_t input_count, BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 1);
	auto &input = inputs[0];
	assert(input.type == TypeId::DATE || input.type == TypeId::TIMESTAMP);

	result.Initialize(TypeId::INTEGER);
	result.nullmask = input.nullmask;
	result.count = input.count;
	result.sel_vector = input.sel_vector;
	auto result_data = (int *)result.data;
	switch (input.type) {
	case TypeId::DATE:
		VectorOperations::ExecType<date_t>(
		    input, [&](date_t date, size_t i, size_t k) { result_data[i] = Date::ExtractYear(date); });
		break;
	case TypeId::TIMESTAMP:
		VectorOperations::ExecType<timestamp_t>(input, [&](timestamp_t timestamp, size_t i, size_t k) {
			result_data[i] = Date::ExtractYear(Timestamp::GetDate(timestamp));
		});
		break;
	default:
		throw NotImplementedException("Can only extract year from dates or timestamps");
	}
}

bool year_matches_arguments(vector<TypeId> &arguments) {
	return arguments.size() == 1 && (arguments[0] == TypeId::DATE || arguments[0] == TypeId::TIMESTAMP);
}

TypeId year_get_return_type(vector<TypeId> &arguments) {
	return TypeId::INTEGER;
}

} // namespace function
} // namespace duckdb
