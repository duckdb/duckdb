#include "function/scalar_function/round.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {
namespace function {

void round_function(Vector inputs[], size_t input_count, BoundFunctionExpression &expr, Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Round(inputs[0], inputs[1], result);
}

bool round_matches_arguments(vector<TypeId> &arguments) {
	return arguments.size() == 2 && TypeIsNumeric(arguments[0]) && TypeIsInteger(arguments[1]);
}

TypeId round_get_return_type(vector<TypeId> &arguments) {
	return arguments[0];
}

} // namespace function
} // namespace duckdb
