#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static void ListTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	// TODO
	
}

static unique_ptr<FunctionData> ListTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments) {

	// TODO:
	// this is about finding out what the input and output types are
	// also, remove the lambda function from the arguments here
	// and add it to the bind info instead

	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction ListTransformFun::GetFunction() {
	// TODO: what logical type is the lambda function?
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR},
	                      LogicalType::LIST(LogicalType::ANY), ListTransformFunction, false, 
						  false, ListTransformBind, nullptr);
}

void ListTransformFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_transform", "array_transform"}, GetFunction());
}

}