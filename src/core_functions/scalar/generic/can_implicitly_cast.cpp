#include "duckdb/core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast_rules.hpp"

namespace duckdb {

bool CanCastImplicitly(ClientContext &context, const LogicalType &source, const LogicalType &target) {
	return CastFunctionSet::Get(context).ImplicitCastCost(source, target) >= 0;
}

static void CanCastImplicitlyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	bool can_cast_implicitly = CanCastImplicitly(context, args.data[0].GetType(), args.data[1].GetType());
	auto v = Value::BOOLEAN(can_cast_implicitly);
	result.Reference(v);
}

unique_ptr<Expression> BindCanCastImplicitlyExpression(FunctionBindExpressionInput &input) {
	auto &source_type = input.function.children[0]->return_type;
	auto &target_type = input.function.children[1]->return_type;
	if (source_type.id() == LogicalTypeId::UNKNOWN || source_type.id() == LogicalTypeId::SQLNULL ||
	    target_type.id() == LogicalTypeId::UNKNOWN || target_type.id() == LogicalTypeId::SQLNULL) {
		// parameter - unknown return type
		return nullptr;
	}
	// emit a constant expression
	return make_uniq<BoundConstantExpression>(
	    Value::BOOLEAN(CanCastImplicitly(input.context, source_type, target_type)));
}

ScalarFunction CanCastImplicitlyFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY, LogicalType::ANY}, LogicalType::BOOLEAN, CanCastImplicitlyFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.bind_expression = BindCanCastImplicitlyExpression;
	return fun;
}

} // namespace duckdb
