
#include "function/scalar_function/abs.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {
namespace function {

void abs_function(Vector inputs[], size_t input_count, Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Abs(inputs[0], result);
}

bool abs_matches_arguments(std::vector<TypeId> &arguments) {
	return arguments.size() == 1 && TypeIsNumeric(arguments[0]);
}

TypeId abs_get_return_type(std::vector<TypeId> &arguments) {
	return arguments[0];
}

} // namespace function
} // namespace duckdb
