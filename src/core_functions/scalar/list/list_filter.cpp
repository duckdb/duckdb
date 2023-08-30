#include "duckdb/core_functions/scalar/list_functions.hpp"

#include "duckdb/core_functions/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

static void ListFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	LambdaFunctions::ExecuteLambda(args, state, result, LambdaType::FILTER);
}

static unique_ptr<FunctionData> ListFilterBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	// at least the list column and the lambda function
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->expression_class != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();

	// try to cast to boolean, if the return type of the lambda filter expression is not already boolean
	if (bound_lambda_expr.lambda_expr->return_type != LogicalType::BOOLEAN) {
		auto cast_lambda_expr =
		    BoundCastExpression::AddCastToType(context, std::move(bound_lambda_expr.lambda_expr), LogicalType::BOOLEAN);
		bound_lambda_expr.lambda_expr = std::move(cast_lambda_expr);
	}

	bound_function.return_type = arguments[0]->return_type;

	if (bound_lambda_expr.parameter_count == 2) {
		return LambdaFunctions::ListLambdaBind(context, bound_function, arguments, bound_lambda_expr.parameter_count,
		                                       true);
	}
	return LambdaFunctions::ListLambdaBind(context, bound_function, arguments, bound_lambda_expr.parameter_count);
}

static LogicalType ListFilterBindLambda(const idx_t parameter_idx, const LogicalType &list_child_type) {

	switch (parameter_idx) {
	case 0:
		return list_child_type;
	case 1:
		return LogicalType::BIGINT;
	default:
		throw BinderException("Lambda function only supports up to two lambda parameters!");
	}
}

ScalarFunction ListFilterFun::GetFunction() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LAMBDA}, LogicalType::LIST(LogicalType::ANY),
	                   ListFilterFunction, ListFilterBind, nullptr, nullptr);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = ListLambdaBindData::Serialize;
	fun.deserialize = ListLambdaBindData::Deserialize;
	fun.format_serialize = ListLambdaBindData::FormatSerialize;
	fun.format_deserialize = ListLambdaBindData::FormatDeserialize;
	fun.bind_lambda = ListFilterBindLambda;
	return fun;
}

} // namespace duckdb
