#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static unique_ptr<FunctionData> JSONTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	if (arguments[1]->return_type == LogicalTypeId::SQLNULL) {
		bound_function.return_type = LogicalTypeId::SQLNULL;
	} else if (!arguments[1]->IsFoldable()) {
		throw InvalidInputException("JSON structure must be a constant!");
	} else {

	}
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

static void TransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
}

CreateScalarFunctionInfo JSONFunctions::GetTransformFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("json_transform", {LogicalType::JSON, LogicalType::JSON},
	                                               LogicalType::ANY, TransformFunction, false, nullptr, nullptr,
	                                               nullptr));
}

} // namespace duckdb
