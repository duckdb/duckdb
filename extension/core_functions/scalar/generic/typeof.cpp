#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

static void TypeOfFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	Value v(args.data[0].GetType().ToString());
	result.Reference(v);
}

unique_ptr<Expression> BindTypeOfFunctionExpression(FunctionBindExpressionInput &input) {
	auto &return_type = input.children[0]->return_type;
	if (return_type.id() == LogicalTypeId::UNKNOWN || return_type.id() == LogicalTypeId::SQLNULL) {
		// parameter - unknown return type
		return nullptr;
	}
	// emit a constant expression
	return make_uniq<BoundConstantExpression>(Value(return_type.ToString()));
}

ScalarFunction TypeOfFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, TypeOfFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.bind_expression = BindTypeOfFunctionExpression;
	return fun;
}

} // namespace duckdb
