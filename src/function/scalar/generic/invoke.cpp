#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

namespace {

struct LambdaInvokeData final : public LambdaFunctionData {
	unique_ptr<Expression> lambda_expr;

	explicit LambdaInvokeData(unique_ptr<Expression> lambda_expr_p) : lambda_expr(std::move(lambda_expr_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto lambda_expr_copy = lambda_expr ? lambda_expr->Copy() : nullptr;
		return make_uniq<LambdaInvokeData>(std::move(lambda_expr_copy));
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LambdaInvokeData>();
		return Expression::Equals(lambda_expr, other.lambda_expr);
	}

	//! Serializes a lambda function's bind data
	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const ScalarFunction &function) {
		auto &bind_data = bind_data_p->Cast<LambdaInvokeData>();
		serializer.WritePropertyWithDefault(101, "lambda_expr", bind_data.lambda_expr, unique_ptr<Expression>());
	}

	//! Deserializes a lambda function's bind data
	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, ScalarFunction &) {
		auto lambda_expr = deserializer.ReadPropertyWithExplicitDefault<unique_ptr<Expression>>(
		    101, "lambda_expr", unique_ptr<Expression>());
		return make_uniq<LambdaInvokeData>(std::move(lambda_expr));
	}

	const unique_ptr<Expression> &GetLambdaExpression() const override {
		return lambda_expr->Cast<BoundLambdaExpression>().lambda_expr;
	}
};

struct LambdaInvokeState final : public FunctionLocalState {
	unique_ptr<ExpressionExecutor> executor;

	explicit LambdaInvokeState(unique_ptr<ExpressionExecutor> executor_p) : executor(std::move(executor_p)) {
	}

	static unique_ptr<FunctionLocalState> Init(ExpressionState &state, const BoundFunctionExpression &expr,
	                                           FunctionData *bind_data) {
		auto &bdata = bind_data->Cast<LambdaInvokeData>();
		auto &bound_lambda_expr = bdata.lambda_expr->Cast<BoundLambdaExpression>();
		auto executor = make_uniq<ExpressionExecutor>(state.GetContext(), *bound_lambda_expr.lambda_expr);
		return make_uniq<LambdaInvokeState>(std::move(executor));
	}
};

void LambdaInvokeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<LambdaInvokeState>();
	lstate.executor->ExecuteExpression(args, result);
}

unique_ptr<FunctionData> LambdaInvokeBind(ClientContext &context, ScalarFunction &bound_function,
                                          vector<unique_ptr<Expression>> &arguments) {
	// the list column and the bound lambda expression
	if (arguments[0]->GetExpressionClass() != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression passed to 'invoke' function.");
	}

	auto &bound_lambda_expr = arguments[0]->Cast<BoundLambdaExpression>();
	if (bound_lambda_expr.parameter_count != arguments.size() - 1) {
		throw BinderException("The number of lambda parameters does not match the number of arguments passed to the "
		                      "'invoke' function, expected %d, got %d.",
		                      bound_lambda_expr.parameter_count, arguments.size() - 1);
	}

	bound_function.SetReturnType(bound_lambda_expr.lambda_expr->return_type);

	return make_uniq<LambdaInvokeData>(bound_lambda_expr.Copy());
}

LogicalType LambdaInvokeBindParameters(ClientContext &context, const vector<LogicalType> &function_child_types,
                                       const idx_t parameter_idx) {
	// The first parameter is always the lambda
	return function_child_types[1 + parameter_idx];
}

} // namespace

ScalarFunction InvokeFun::GetFunction() {
	ScalarFunction fun("invoke", {LogicalType::LAMBDA, LogicalType::ANY}, LogicalType::ANY, LambdaInvokeFunction);
	fun.bind = LambdaInvokeBind;
	fun.varargs = LogicalType::ANY;
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetBindLambdaCallback(LambdaInvokeBindParameters);
	fun.SetInitStateCallback(LambdaInvokeState::Init);
	fun.SetSerializeCallback(LambdaInvokeData::Serialize);
	fun.SetDeserializeCallback(LambdaInvokeData::Deserialize);
	return fun;
}

} // namespace duckdb
