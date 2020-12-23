#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

static void alias_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	Value v(state.expr.alias.empty() ? func_expr.children[0]->GetName() : state.expr.alias);
	result.Reference(v);
}

void AliasFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("alias", {LogicalType::ANY}, LogicalType::VARCHAR, alias_function));
}

} // namespace duckdb
