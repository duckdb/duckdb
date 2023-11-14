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

	// NULL list parameter
	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_uniq<ListLambdaBindData>(bound_function.return_type, nullptr);
	}
	// prepared statements
	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));
	D_ASSERT(arguments[0]->return_type.id() == LogicalTypeId::LIST);

	// get the lambda expression and put it in the bind info

	auto list_child_type = arguments[0]->return_type;
	list_child_type = ListType::GetChildType(list_child_type).id();

	auto cast_lambda_expr = BoundCastExpression::AddCastToType(context, std::move(bound_lambda_expr.lambda_expr), list_child_type, true);
	if (!cast_lambda_expr) {
		throw BinderException("Result type of the lambda expression must be implicitly castable to the list type");
	}

	auto lambda_expr = std::move(cast_lambda_expr->Cast<BoundCastExpression>().child);


	return make_uniq<ListLambdaBindData>(bound_function.return_type, std::move(lambda_expr), has_index);
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
