#include "function/scalar_function/math_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void cbrt_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 1);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::CbRt(inputs[0], result);
}

}
