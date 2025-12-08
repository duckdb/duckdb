#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {
namespace {
struct SwitchFunctionBindData : FunctionData {
	explicit SwitchFunctionBindData(Value value_p) : value(std::move(value_p)) {

	}

	Value value;
	bool Equals(const FunctionData &other_p) const override {
		const auto &other = other_p.Cast<SwitchFunctionBindData>();
		return Value::NotDistinctFrom(value, other.value);
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SwitchFunctionBindData>(value);
	}
};

unique_ptr<FunctionData> SwitchExpressionBind(ClientContext &context, ScalarFunction &function,
                                         vector<unique_ptr<Expression>> &arguments) {
	auto default_value = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
	function.SetReturnType(default_value.type());
	return make_uniq<SwitchFunctionBindData>(std::move(default_value));
}

void ExtractConstantExprFromList(unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &result) {
	if (expr->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		throw BinderException("Expected a function for the cases");
	}
	auto &list_function = expr->Cast<BoundFunctionExpression>();
	if (list_function.function.name != "list_value") {
		throw BinderException("Expected a list function");
	}
	for (auto &list_child : list_function.children) {
		if (list_child->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			throw BinderException("Expected a map with constant values");
		}
		result.push_back(std::move(list_child));
	}
}

unique_ptr<Expression> SwitchBindDefaultExpression(FunctionBindExpressionInput &input) {
	auto function_data = input.bind_data->Cast<SwitchFunctionBindData>();
	auto result = make_uniq<BoundCaseExpression>(function_data.value.type());
	auto &base = input.children[0];
	auto &cases = input.children[1];
	if (cases->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		throw BinderException("Expected a map function for the cases");
	}
	auto &func_cases = cases->Cast<BoundFunctionExpression>();
	if (func_cases.function.name != "map") {
		throw BinderException("Expected a map function for the cases");
	}

	auto &map_children = func_cases.children;
	D_ASSERT(map_children.size() == 2);

	vector<unique_ptr<Expression>> keys_unpacked;
	vector<unique_ptr<Expression>> values_unpacked;
	ExtractConstantExprFromList(map_children[0], keys_unpacked);
	ExtractConstantExprFromList(map_children[1], values_unpacked);

	BoundCaseCheck case_check;
	for (idx_t i = 0; i < keys_unpacked.size(); i++) {
		case_check.when_expr = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, base->Copy(), std::move(keys_unpacked[i]));
		case_check.then_expr = std::move(values_unpacked[i]);
		result->case_checks.push_back(std::move(case_check));
	}
	auto &default_case = input.children[2];
	result->else_expr = std::move(default_case);
	return std::move(result);
}

unique_ptr<Expression> SwitchBindMissingDefaultExpression(FunctionBindExpressionInput &input) {
	Printer::Print("Got here and then probably crash");

	throw InternalException("Switch function got to the bind expression");
	return make_uniq<BoundCaseExpression>(LogicalType::SQLNULL);
}
} // namespace

ScalarFunctionSet SwitchFun::GetFunctions() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");
	ScalarFunctionSet func_set;
	// auto switch_missing_default = ScalarFunction({key_type, LogicalType::MAP(key_type, val_type)},
	// 	val_type, nullptr, SwitchExpressionBind, nullptr);
	// switch_missing_default.SetBindExpressionCallback(SwitchBindMissingDefaultExpression);
	// func_set.AddFunction(std::move(switch_missing_default));
	auto switch_missing = ScalarFunction({key_type, LogicalType::MAP(key_type, val_type), val_type}, val_type,
	                   nullptr, SwitchExpressionBind, nullptr);
	switch_missing.SetBindExpressionCallback(SwitchBindDefaultExpression);
	func_set.AddFunction(std::move(switch_missing));
	return func_set;
}
}
