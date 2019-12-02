#include "duckdb/function/scalar/trigonometric_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/exception.hpp"

using namespace std;

namespace duckdb {

static void asin_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                          Vector &result) {
	assert(input_count == 1);
	inputs[0].Cast(TypeId::DOUBLE);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::ASin(inputs[0], result);
}

void AsinFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("asin", {SQLType::DOUBLE}, SQLType::DOUBLE, asin_function));
}

} // namespace duckdb
