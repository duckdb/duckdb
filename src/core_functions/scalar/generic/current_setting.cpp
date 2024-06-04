#include "duckdb/core_functions/scalar/generic_functions.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/catalog/catalog.hpp"
namespace duckdb {

struct CurrentSettingBindData : public FunctionData {
	explicit CurrentSettingBindData(Value value_p) : value(std::move(value_p)) {
	}

	Value value;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CurrentSettingBindData>(value);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CurrentSettingBindData>();
		return Value::NotDistinctFrom(value, other.value);
	}
};

static void CurrentSettingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<CurrentSettingBindData>();
	result.Reference(info.value);
}

unique_ptr<FunctionData> CurrentSettingBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {

	auto &key_child = arguments[0];
	if (key_child->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	if (key_child->return_type.id() != LogicalTypeId::VARCHAR ||
	    key_child->return_type.id() != LogicalTypeId::VARCHAR || !key_child->IsFoldable()) {
		throw ParserException("Key name for current_setting needs to be a constant string");
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(context, *key_child);
	D_ASSERT(key_val.type().id() == LogicalTypeId::VARCHAR);
	if (key_val.IsNull() || StringValue::Get(key_val).empty()) {
		throw ParserException("Key name for current_setting needs to be neither NULL nor empty");
	}

	auto key = StringUtil::Lower(StringValue::Get(key_val));
	Value val;
	if (!context.TryGetCurrentSetting(key, val)) {
		Catalog::AutoloadExtensionByConfigName(context, key);
		// If autoloader didn't throw, the config is now available
		context.TryGetCurrentSetting(key, val);
	}

	bound_function.return_type = val.type();
	return make_uniq<CurrentSettingBindData>(val);
}

ScalarFunction CurrentSettingFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::VARCHAR}, LogicalType::ANY, CurrentSettingFunction, CurrentSettingBind);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
