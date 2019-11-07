#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

static void abs_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                         Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Abs(inputs[0], result);
}

void AbsFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet abs("abs");
	for (auto &type : SQLType::NUMERIC) {
		abs.AddFunction(ScalarFunction({type}, type, abs_function));
	}
	set.AddFunction(abs);
}
