#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void sign_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                          Vector &result) {
	assert(input_count == 1);
	result.Initialize(TypeId::TINYINT);
	VectorOperations::Sign(inputs[0], result);
}

void SignFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet sign("sign");
	for (auto &type : SQLType::NUMERIC) {
		sign.AddFunction(ScalarFunction({type}, SQLType::TINYINT, sign_function));
	}
	set.AddFunction(sign);
}

} // namespace duckdb
