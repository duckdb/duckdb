#include <string>

#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class DataChunk;

static void AliasFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	Value v(state.expr.GetAlias().empty() ? func_expr.children[0]->GetName() : state.expr.GetAlias());
	result.Reference(v, count_t(args.size()));
}

ScalarFunction AliasFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, AliasFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
