#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

static void TryExpressionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(args.data[0]);
}

static unique_ptr<FunctionData> TryExpressionBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 1) {
		throw BinderException("TRY only accepts a single expression as input");
	}
	auto return_type = ExpressionBinder::GetExpressionReturnType(*arguments[0]);

	bound_function.return_type = return_type;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction TryFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::ANY, TryExpressionFunction, TryExpressionBind);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
