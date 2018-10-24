
#include "function/scalar_function/year.hpp"

#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/types/vector_operations.hpp"

using namespace std;

namespace duckdb {
namespace function {

void year_function(Vector inputs[], size_t input_count, Vector &result) {
	assert(input_count == 1);
	auto &input = inputs[0];
	assert(input.type == TypeId::DATE);

	result.Initialize(TypeId::INTEGER);
	result.nullmask = input.nullmask;
	result.count = input.count;
	result.sel_vector = input.sel_vector;
	auto result_data = (int *)result.data;
	VectorOperations::ExecType<date_t>(
	    input, [&](date_t date, size_t i, size_t k) {
		    result_data[i] = Date::ExtractYear(date);
	    });
}

bool year_matches_arguments(std::vector<TypeId> &arguments) {
	return arguments.size() == 1 && arguments[0] == TypeId::DATE;
}

TypeId year_get_return_type(std::vector<TypeId> &arguments) {
	return TypeId::INTEGER;
}

} // namespace function
} // namespace duckdb
