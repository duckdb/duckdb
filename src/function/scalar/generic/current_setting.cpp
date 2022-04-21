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
	unique_ptr<FunctionData> Copy() const override {
		return make_unique<CurrentSettingBindData>(value);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const CurrentSettingBindData &)other_p;
		return Value::NotDistinctFrom(value, other.value);
	}
};

static void CurrentSettingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (CurrentSettingBindData &)*func_expr.bind_info;
	result.Reference(info.value);
}

unique_ptr<FunctionData> CurrentSettingBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {

	auto &key_child = arguments[0];

	if (key_child->return_type.id() != LogicalTypeId::VARCHAR ||
	    key_child->return_type.id() != LogicalTypeId::VARCHAR || !key_child->IsFoldable()) {
		throw ParserException("Key name for current_setting needs to be a constant string");
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(*key_child.get());
	D_ASSERT(key_val.type().id() == LogicalTypeId::VARCHAR);
	auto &key_str = StringValue::Get(key_val);
	if (key_val.IsNull() || key_str.empty()) {
		throw ParserException("Key name for current_setting needs to be neither NULL nor empty");
	}

	auto key = StringUtil::Lower(key_str);
	Value val;
	if (!context.TryGetCurrentSetting(key, val)) {
		throw InvalidInputException("unrecognized configuration parameter \"%s\"", key_str);
	}

	bound_function.return_type = val.type();
	return make_unique<CurrentSettingBindData>(val);
}

void CurrentSettingFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("current_setting", {LogicalType::VARCHAR}, LogicalType::ANY, CurrentSettingFunction,
	                               false, CurrentSettingBind));
}

} // namespace duckdb
