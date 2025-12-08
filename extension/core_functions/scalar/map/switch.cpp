#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {
namespace {
struct SwitchFunctionBindData : FunctionData {
	explicit SwitchFunctionBindData(LogicalType return_type_p) : return_type(std::move(return_type_p)) {

	}

	LogicalType return_type;
	bool Equals(const FunctionData &other_p) const override {
		const auto &other = other_p.Cast<SwitchFunctionBindData>();
		return return_type == other.return_type;
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SwitchFunctionBindData>(return_type);
	}
};

unique_ptr<FunctionData> SwitchExpressionBind(ClientContext &context, ScalarFunction &function,
                                         vector<unique_ptr<Expression>> &arguments) {
	auto map_value = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	auto values_type = MapType::ValueType(map_value.type());
	return make_uniq<SwitchFunctionBindData>(values_type);
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

void ConstructCaseChecks(unique_ptr<Expression> &keys, unique_ptr<Expression> &values, vector<BoundCaseCheck> &case_checks, optional_ptr<Expression> base) {
	vector<unique_ptr<Expression>> keys_unpacked;
	vector<unique_ptr<Expression>> values_unpacked;
	ExtractConstantExprFromList(keys, keys_unpacked);
	ExtractConstantExprFromList(values, values_unpacked);
	case_checks.reserve(keys_unpacked.size());
	BoundCaseCheck case_check;
	for (idx_t i = 0; i < keys_unpacked.size(); i++) {
		if (base) {
			case_check.when_expr = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, base->Copy(), std::move(keys_unpacked[i]));
		} else {
			case_check.when_expr = std::move(keys_unpacked[i]);
		}
		case_check.then_expr = std::move(values_unpacked[i]);
		case_checks.push_back(std::move(case_check));
	}
}

unique_ptr<Expression> SwitchBindDefaultExpression(FunctionBindExpressionInput &input) {
	auto function_data = input.bind_data->Cast<SwitchFunctionBindData>();
	auto result = make_uniq<BoundCaseExpression>(function_data.return_type);
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
	ConstructCaseChecks(map_children[0], map_children[1], result->case_checks, optional_ptr<Expression>(base));
	auto &default_case = input.children[2];
	result->else_expr = std::move(default_case);
	return std::move(result);
}

unique_ptr<Expression> SwitchBindMissingDefaultExpression(FunctionBindExpressionInput &input) {
	auto function_data = input.bind_data->Cast<SwitchFunctionBindData>();
	auto result = make_uniq<BoundCaseExpression>(function_data.return_type);
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
	ConstructCaseChecks(map_children[0], map_children[1], result->case_checks, optional_ptr<Expression>(base));
	result->else_expr = BoundCastExpression::AddCastToType(input.context, make_uniq<BoundConstantExpression>(Value()), function_data.return_type);
	return std::move(result);
}
} // namespace

ScalarFunctionSet SwitchFun::GetFunctions() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");
	ScalarFunctionSet func_set;
	auto switch_missing_default = ScalarFunction({key_type, LogicalType::MAP(key_type, val_type)},
		val_type, nullptr, SwitchExpressionBind, nullptr);
	switch_missing_default.SetBindExpressionCallback(SwitchBindMissingDefaultExpression);
	func_set.AddFunction(std::move(switch_missing_default));
	auto switch_missing = ScalarFunction({key_type, LogicalType::MAP(key_type, val_type), val_type}, val_type,
	                   nullptr, SwitchExpressionBind, nullptr);
	switch_missing.SetBindExpressionCallback(SwitchBindDefaultExpression);
	func_set.AddFunction(std::move(switch_missing));
	return func_set;
}
}
