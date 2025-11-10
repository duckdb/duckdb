#include "core_functions/scalar/list_functions.hpp"

#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

static unique_ptr<FunctionData> ListTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	// the list column and the bound lambda expression
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->GetExpressionClass() != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	bound_function.SetReturnType(LogicalType::LIST(bound_lambda_expr.lambda_expr->return_type));
	auto has_index = bound_lambda_expr.parameter_count == 2;
	return LambdaFunctions::ListLambdaBind(context, bound_function, arguments, has_index);
}

static LogicalType ListTransformBindLambda(ClientContext &context, const vector<LogicalType> &function_child_types,
                                           const idx_t parameter_idx) {
	return LambdaFunctions::BindBinaryChildren(function_child_types, parameter_idx);
}

ScalarFunction ListTransformFun::GetFunction() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LAMBDA}, LogicalType::LIST(LogicalType::ANY),
	                   LambdaFunctions::ListTransformFunction, ListTransformBind, nullptr, nullptr);

	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.serialize = ListLambdaBindData::Serialize;
	fun.deserialize = ListLambdaBindData::Deserialize;
	fun.bind_lambda = ListTransformBindLambda;

	return fun;
}

} // namespace duckdb
