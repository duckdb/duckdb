#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/core_functions/lambda_functions.hpp"


#include "duckdb/core_functions/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"


namespace duckdb {

static unique_ptr<FunctionData> ListReduceBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {

	// the list column and the bound lambda expression
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->expression_class != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	bound_function.return_type = bound_lambda_expr.lambda_expr->return_type;
	auto has_index = bound_lambda_expr.parameter_count == 3;
	return LambdaFunctions::ListLambdaBind(context, bound_function, arguments, has_index);
}

static LogicalType ListReduceBindLambda(const idx_t parameter_idx, const LogicalType &list_child_type) {
	return LambdaFunctions::BindReduceLambda(parameter_idx, list_child_type);
}

ScalarFunction ListReduceFun::GetFunction() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LAMBDA}, LogicalType::ANY,
	                   LambdaFunctions::ListReduceFunction, ListReduceBind, nullptr, nullptr);

	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = ListLambdaBindData::Serialize;
	fun.deserialize = ListLambdaBindData::Deserialize;
	fun.bind_lambda = ListReduceBindLambda;

	return fun;
}

} // namespace duckdb
