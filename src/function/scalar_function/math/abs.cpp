#include "function/scalar_function/math_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void abs_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Abs(inputs[0], result);
}

}
