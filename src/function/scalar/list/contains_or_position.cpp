#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"

namespace duckdb {

static void ListPositionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	(void)state;
	return ListContainsOrPosition<int32_t, PositionFunctor, ListArgFunctor>(args, result);
}

static unique_ptr<FunctionData> ListPositionBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	return ListContainsOrPositionBind<LogicalType::INTEGER>(context, bound_function, arguments);
}

ScalarFunction ListPositionFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, // argument list
	                      LogicalType::INTEGER,                                    // return type
	                      ListPositionFunction, ListPositionBind, nullptr);
}

void ListPositionFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_position", "list_indexof", "array_position", "array_indexof"}, GetFunction());
}
} // namespace duckdb
