#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct CurrentSettingBindData : public FunctionData {

	Value value;

	CurrentSettingBindData(Value value_p) : value(value_p) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<CurrentSettingBindData>(value);
	}
};

static void current_setting_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (CurrentSettingBindData &)*func_expr.bind_info;
	result.Reference(info.value);
}

unique_ptr<FunctionData> current_setting_bind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {

	auto &key_child = arguments[0];

	if (key_child->return_type.id() != LogicalTypeId::VARCHAR ||
	    key_child->return_type.id() != LogicalTypeId::VARCHAR || !key_child->IsFoldable()) {
		throw Exception("Key name for struct_extract needs to be a constant string");
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(*key_child.get());
	D_ASSERT(key_val.type().id() == LogicalTypeId::VARCHAR);
	if (key_val.is_null || key_val.str_value.length() < 1) {
		throw Exception("Key name for struct_extract needs to be neither NULL nor empty");
	}

	if (context.set_variables.find(key_val.str_value) == context.set_variables.end()) {
		throw InvalidInputException("Variable '%s' was not SET in this context", key_val.str_value);
	}
	Value val = context.set_variables[key_val.str_value];
	bound_function.return_type = val.type();
	return make_unique<CurrentSettingBindData>(val);
}

void CurrentSettingFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("current_setting", {LogicalType::VARCHAR}, LogicalType::ANY,
	                               current_setting_function, false, current_setting_bind));
}

} // namespace duckdb
