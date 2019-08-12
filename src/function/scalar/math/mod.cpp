#include "function/scalar/math_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "planner/expression/bound_function_expression.hpp"

using namespace std;

namespace duckdb {

static void mod_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr, Vector &result) {
	result.Initialize(expr.return_type);
	inputs[0].Cast(expr.return_type);
	inputs[1].Cast(expr.return_type);
	VectorOperations::Modulo(inputs[0], inputs[1], result);
}

void Mod::RegisterFunction(BuiltinFunctions &set) {
	FunctionSet mod("mod");
	for(auto &type : SQLType::NUMERIC) {
		mod.AddFunction(ScalarFunction({ type, type }, type, mod_function));
	}
	set.AddFunction(mod);
}

}
