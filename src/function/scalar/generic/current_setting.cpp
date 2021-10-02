#include "duckdb/function/scalar/generic_functions.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

struct CurrentSettingBindData : public FunctionData {
	explicit CurrentSettingBindData(Value value_p) : value(move(value_p)) {
	}

	Value value;

public:
	unique_ptr<FunctionData> Copy() override {
		return make_unique<CurrentSettingBindData>(value);
	}
};

static void CurrentSettingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (CurrentSettingBindData &)*func_expr.bind_info;
	result.Reference(info.value);
}

static string NormalizeSettingName(const string &name) {
	if (CaseInsensitiveStringEquality()(name, "schema")) {
		// The PG doc says:
		// >  SET SCHEMA 'value' is an alias for SET search_path TO value.
		return "search_path";
	}

	return name;
}

unique_ptr<FunctionData> CurrentSettingBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {

	auto &key_child = arguments[0];

	if (key_child->return_type.id() != LogicalTypeId::VARCHAR ||
	    key_child->return_type.id() != LogicalTypeId::VARCHAR || !key_child->IsFoldable()) {
		throw ParserException("Key name for struct_extract needs to be a constant string");
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(*key_child.get());
	D_ASSERT(key_val.type().id() == LogicalTypeId::VARCHAR);
	if (key_val.is_null || key_val.str_value.length() < 1) {
		throw ParserException("Key name for struct_extract needs to be neither NULL nor empty");
	}

	const auto &key = NormalizeSettingName(key_val.str_value);
	Value val;
	if (!context.TryGetCurrentSetting(key, val)) {
		throw InvalidInputException("Variable '%s' was not SET in this context", key);
	}

	bound_function.return_type = val.type();
	return make_unique<CurrentSettingBindData>(val);
}

void CurrentSettingFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("current_setting", {LogicalType::VARCHAR}, LogicalType::ANY, CurrentSettingFunction,
	                               false, CurrentSettingBind));
}

} // namespace duckdb
