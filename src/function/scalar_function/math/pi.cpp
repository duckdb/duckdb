#include "function/scalar_function/math_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void pi_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                 Vector &result) {
	assert(input_count == 0);
	result.Initialize(TypeId::DOUBLE);
	result.count = 1;
	VectorOperations::Set(result, Value(PI));
}

}
