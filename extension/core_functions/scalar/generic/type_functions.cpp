#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// typeof function
//----------------------------------------------------------------------------------------------------------------------

static void TypeOfFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	Value v(args.data[0].GetType().ToString());
	result.Reference(v, count_t(args.size()));
}

static unique_ptr<Expression> BindTypeOfFunctionExpression(FunctionBindExpressionInput &input) {
	auto &return_type = input.children[0]->GetReturnType();
	if (return_type.id() == LogicalTypeId::UNKNOWN || return_type.id() == LogicalTypeId::SQLNULL) {
		// parameter - unknown return type
		return nullptr;
	}
	// emit a constant expression
	return make_uniq<BoundConstantExpression>(Value(return_type.ToString()));
}

ScalarFunction TypeOfFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, TypeOfFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetBindExpressionCallback(BindTypeOfFunctionExpression);
	return fun;
}

//----------------------------------------------------------------------------------------------------------------------
// get_type	function
//----------------------------------------------------------------------------------------------------------------------
// This is like "typeof", except returns LogicalType::TYPE instead of VARCHAR

static void GetTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto v = Value::TYPE(args.data[0].GetType());
	result.Reference(v, count_t(args.size()));
}

static unique_ptr<FunctionData> BindGetTypeFunction(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	bound_function.GetArguments()[0] = arguments[0]->GetReturnType();
	return nullptr;
}

static unique_ptr<Expression> BindGetTypeFunctionExpression(FunctionBindExpressionInput &input) {
	auto &return_type = input.children[0]->GetReturnType();
	if (return_type.id() == LogicalTypeId::UNKNOWN || return_type.id() == LogicalTypeId::SQLNULL) {
		// parameter - unknown return type
		return nullptr;
	}
	// emit a constant expression
	return make_uniq<BoundConstantExpression>(Value::TYPE(return_type));
}

ScalarFunction GetTypeFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::TYPE(), GetTypeFunction, BindGetTypeFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetBindExpressionCallback(BindGetTypeFunctionExpression);
	return fun;
}

//----------------------------------------------------------------------------------------------------------------------
// make_type function
//----------------------------------------------------------------------------------------------------------------------
static void MakeTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InvalidInputException("make_type function can only be used in constant expressions");
}

//! Turn the arguments an "*args"/"**kwargs" pack collected into type arguments. A keyword pack keys them by name,
//! a positional one leaves them unnamed - which is what tells apart LIST(INTEGER) from STRUCT(a INTEGER).
static void AddTypeArguments(ClientContext &context, Expression &pack,
                             vector<unique_ptr<ParsedExpression>> &type_args) {
	if (!pack.IsFoldable()) {
		throw BinderException("make_type function arguments must be constant expressions");
	}
	auto pack_value = ExpressionExecutor::EvaluateScalar(context, pack);
	auto &pack_items = StructValue::GetChildren(pack_value);

	for (idx_t i = 0; i < pack_items.size(); i++) {
		auto type_arg = make_uniq<ConstantExpression>(pack_items[i]);
		type_arg->SetAlias(StructType::GetChildName(pack_value.type(), i));
		type_args.push_back(std::move(type_arg));
	}
}

static unique_ptr<Expression> BindMakeTypeFunctionExpression(FunctionBindExpressionInput &input) {
	auto &name_arg = input.children[0];

	if (!name_arg->IsFoldable()) {
		throw BinderException("make_type function arguments must be constant expressions");
	}
	auto name_val = ExpressionExecutor::EvaluateScalar(input.context, *name_arg);
	if (name_val.IsNull()) {
		throw BinderException("make_type function type_name argument must not be NULL");
	}
	auto &type_name = StringValue::Get(name_val);

	vector<unique_ptr<ParsedExpression>> type_args;
	AddTypeArguments(input.context, *input.children[1], type_args);
	AddTypeArguments(input.context, *input.children[2], type_args);

	auto qualified_name = QualifiedName::Parse(type_name);

	auto unbound_type =
	    LogicalType::UNBOUND(make_uniq<TypeExpression>(std::move(qualified_name), std::move(type_args)));

	// Bind the unbound type
	auto binder = Binder::CreateBinder(input.context);
	binder->BindLogicalType(unbound_type);
	return make_uniq<BoundConstantExpression>(Value::TYPE(unbound_type));
}

ScalarFunction MakeTypeFun::GetFunction() {
	auto sig = FunctionSignature()
	               .AddParameter("type_name", LogicalType::VARCHAR)
	               .AddVarPositionalParameter("args", LogicalTypeId::ANY)
	               .AddVarKeywordParameter("kwargs", LogicalTypeId::ANY)
	               .SetReturnType(LogicalType::TYPE());

	auto fun = ScalarFunction("make_type", std::move(sig))
	               .SetFunctionCallback(MakeTypeFunction)
	               .SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING)
	               .SetBindExpressionCallback(BindMakeTypeFunctionExpression);

	return fun;
}

} // namespace duckdb
