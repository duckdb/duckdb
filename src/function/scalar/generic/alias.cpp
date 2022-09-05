#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

static void AliasFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	Value v(state.expr.alias.empty() ? func_expr.children[0]->GetName() : state.expr.alias);
	result.Reference(v);
}

void AliasFun::RegisterFunction(BuiltinFunctions &set) {
	auto fun = ScalarFunction("alias", {LogicalType::ANY}, LogicalType::VARCHAR, AliasFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(fun);
}

} // namespace duckdb
