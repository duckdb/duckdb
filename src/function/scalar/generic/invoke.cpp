#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"

#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"

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
	                      const BoundScalarFunction &function) {
		auto &bind_data = bind_data_p->Cast<LambdaInvokeData>();
		serializer.WritePropertyWithDefault(101, "lambda_expr", bind_data.lambda_expr, unique_ptr<Expression>());
	}

	//! Deserializes a lambda function's bind data
	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, BoundScalarFunction &) {
		auto lambda_expr = deserializer.ReadPropertyWithExplicitDefault<unique_ptr<Expression>>(
		    101, "lambda_expr", unique_ptr<Expression>());
		return make_uniq<LambdaInvokeData>(std::move(lambda_expr));
	}

	optional_ptr<const Expression> GetLambdaExpression() const override {
		if (!lambda_expr) {
			return nullptr;
		}
		auto &bound_lambda_expr = lambda_expr->Cast<BoundLambdaExpression>();
		return bound_lambda_expr.LambdaExpr().get();
	}
};

struct LambdaInvokeState final : public FunctionLocalState {
	unique_ptr<ExpressionExecutor> executor;
	DataChunk input_chunk;
	idx_t parameter_count;

	LambdaInvokeState(unique_ptr<ExpressionExecutor> executor_p, const vector<LogicalType> &input_types,
	                  const idx_t parameter_count_p)
	    : executor(std::move(executor_p)), parameter_count(parameter_count_p) {
		input_chunk.InitializeEmpty(input_types);
	}

	static unique_ptr<FunctionLocalState> Init(ExpressionState &state, const BoundFunctionExpression &expr,
	                                           FunctionData *bind_data) {
		auto &bdata = bind_data->Cast<LambdaInvokeData>();
		if (!bdata.lambda_expr) {
			throw InternalException("Invoke function is missing its bound lambda expression");
		}
		auto &bound_lambda_expr = bdata.lambda_expr->Cast<BoundLambdaExpression>();
		const auto parameter_count = bound_lambda_expr.ParameterCount();

		// the trailing arguments are collected by a "*args" parameter, whose type says what it holds
		vector<LogicalType> argument_types;
		for (auto &child : expr.GetChildren()) {
			auto &child_type = child->GetReturnType();
			if (!ArgumentPack::IsPackType(child_type)) {
				argument_types.push_back(child_type);
				continue;
			}
			for (idx_t i = 0; i < StructType::GetChildCount(child_type); i++) {
				argument_types.push_back(StructType::GetChildType(child_type, i));
			}
		}

		D_ASSERT(parameter_count <= argument_types.size());

		vector<LogicalType> input_types;
		input_types.reserve(argument_types.size());
		for (idx_t i = 0; i < parameter_count; i++) {
			input_types.push_back(argument_types[parameter_count - i - 1]);
		}
		for (idx_t i = parameter_count; i < argument_types.size(); i++) {
			input_types.push_back(argument_types[i]);
		}

		auto executor = make_uniq<ExpressionExecutor>(state.GetContext(), *bound_lambda_expr.LambdaExpr());
		return make_uniq<LambdaInvokeState>(std::move(executor), input_types, parameter_count);
	}
};

void LambdaInvokeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<LambdaInvokeState>();

	vector<reference<Vector>> inputs;
	for (auto &column : args.data) {
		if (!ArgumentPack::IsPackType(column.GetType())) {
			inputs.push_back(column);
			continue;
		}
		for (auto &packed : StructVector::GetEntries(column)) {
			inputs.push_back(packed);
		}
	}

	for (idx_t i = 0; i < lstate.parameter_count; i++) {
		lstate.input_chunk.data[i].Reference(inputs[lstate.parameter_count - i - 1].get());
	}
	for (idx_t i = lstate.parameter_count; i < inputs.size(); i++) {
		lstate.input_chunk.data[i].Reference(inputs[i].get());
	}
	lstate.input_chunk.SetChildCardinality(args.size());
	lstate.executor->ExecuteExpression(lstate.input_chunk, result);
}

unique_ptr<FunctionData> LambdaInvokeBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto arguments = input.GetUnpackedArguments();
	// the list column and the bound lambda expression
	if (arguments[0].get().GetExpressionClass() != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression passed to 'invoke' function.");
	}

	auto &bound_lambda_expr = arguments[0].get().Cast<BoundLambdaExpression>();
	if (bound_lambda_expr.ParameterCount() != arguments.size() - 1) {
		throw BinderException("The number of lambda parameters does not match the number of arguments passed to the "
		                      "'invoke' function, expected %d, got %d.",
		                      bound_lambda_expr.ParameterCount(), arguments.size() - 1);
	}

	bound_function.SetReturnType(bound_lambda_expr.LambdaExpr()->GetReturnType());

	return make_uniq<LambdaInvokeData>(bound_lambda_expr.Copy());
}

LogicalType LambdaInvokeBindParameters(ClientContext &context, const vector<LogicalType> &function_child_types,
                                       const idx_t parameter_idx, optional_ptr<BindLambdaContext> bind_lambda_context) {
	// The first parameter is always the lambda
	auto child_idx = parameter_idx + 1;
	if (child_idx >= function_child_types.size()) {
		throw BinderException("The number of lambda parameters does not match the number of arguments passed to the "
		                      "'invoke' function, expected at least %d, got %d.",
		                      parameter_idx + 1, function_child_types.size() - 1);
	}
	return function_child_types[child_idx];
}

} // namespace

ScalarFunction InvokeFun::GetFunction() {
	FunctionSignature signature;
	signature.AddParameter("lambda", LogicalType::LAMBDA);
	signature.AddParameter("arg", LogicalType::ANY);
	signature.AddVarPositionalParameter("args", LogicalType::ANY);
	signature.SetReturnType(LogicalType::ANY);
	ScalarFunction fun("invoke", std::move(signature), LambdaInvokeFunction);
	fun.SetBindCallback(LambdaInvokeBind);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetBindLambdaCallback(LambdaInvokeBindParameters);
	fun.SetInitStateCallback(LambdaInvokeState::Init);
	fun.SetSerializeCallback(LambdaInvokeData::Serialize);
	fun.SetDeserializeCallback(LambdaInvokeData::Deserialize);
	return fun;
}

} // namespace duckdb
