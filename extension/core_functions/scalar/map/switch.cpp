#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {
namespace {
struct SwitchFunctionBindData : FunctionData {
	explicit SwitchFunctionBindData(const LogicalType &return_type_p, idx_t map_index_p)
	    : return_type(std::move(return_type_p)), map_index(std::move(map_index_p)) {
	}

	LogicalType return_type;
	idx_t map_index;

	bool Equals(const FunctionData &other_p) const override {
		const auto &other = other_p.Cast<SwitchFunctionBindData>();
		if (return_type != other.return_type) {
			return false;
		}
		if (map_index != other.map_index) {
			return false;
		}
		return true;
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SwitchFunctionBindData>(return_type, map_index);
	}
};

idx_t FindMapArgumentIndex(const vector<unique_ptr<Expression>> &arguments) {
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i]->return_type.id() == LogicalTypeId::MAP) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

unique_ptr<FunctionData> SwitchBindReturnType(ClientContext &context, ScalarFunction &function,
                                              vector<unique_ptr<Expression>> &arguments) {
	auto map_index = FindMapArgumentIndex(arguments);
	if (map_index == DConstants::INVALID_INDEX) {
		throw BinderException("Switch: No map argument found");
	}
	auto &cases = arguments[map_index];
	if (cases->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		throw BinderException("SWITCH expected a constant map for the cases");
	}
	auto &func = cases->Cast<BoundFunctionExpression>();
	if (func.function.name != "map") {
		throw BinderException("SWITCH expected a constant map for the cases");
	}
	auto map_value = ExpressionExecutor::EvaluateScalar(context, *cases);
	auto values_type = MapType::ValueType(map_value.type());
	return make_uniq<SwitchFunctionBindData>(values_type, map_index);
}

void ExtractConstantExprFromList(unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &result) {
	if (expr->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		throw BinderException("Expected a function for the cases");
	}
	auto &list_function = expr->Cast<BoundFunctionExpression>();
	if (list_function.function.name != "list_value") {
		throw BinderException("Expected a list function");
	}
	if (list_function.children.empty()) {
		throw BinderException("No values provided for SWITCH expression");
	}
	for (auto &list_child : list_function.children) {
		if (list_child->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			throw NotImplementedException("Only constant expressions are supported for keys inside SWITCH");
		}
		result.push_back(std::move(list_child));
	}
}

unique_ptr<Expression> SwitchBindExpression(FunctionBindExpressionInput &input) {
	auto function_data = input.bind_data->Cast<SwitchFunctionBindData>();
	auto result = make_uniq<BoundCaseExpression>(function_data.return_type);
	idx_t map_index = function_data.map_index;
	unique_ptr<Expression> base_expr = nullptr;
	unique_ptr<Expression> default_expr = nullptr;

	if (map_index == 1) {
		base_expr = std::move(input.children[0]);
	}

	if (input.children.size() > map_index + 1) {
		// If there is an argument after the map_index, we have a default expression
		default_expr = std::move(input.children[map_index + 1]);
	}
	unique_ptr<Expression> cases;
	if (input.children[map_index]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		cases = std::move(input.children[map_index]);
	} else if (input.children[map_index]->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast_expr = input.children[map_index]->Cast<BoundCastExpression>();
		if (cast_expr.child->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
			throw BinderException("SWITCH expected a map function for the cases");
		}
		cases = std::move(cast_expr.child);
	} else {
		throw BinderException("SWITCH expected a map function for the cases");
	}
	auto &cases_func = cases->Cast<BoundFunctionExpression>();
	D_ASSERT(cases_func.children.size() == 2);

	vector<unique_ptr<Expression>> keys_unpacked;
	vector<unique_ptr<Expression>> values_unpacked;
	ExtractConstantExprFromList(cases_func.children[0], keys_unpacked);
	ExtractConstantExprFromList(cases_func.children[1], values_unpacked);

	result->case_checks.reserve(keys_unpacked.size());
	BoundCaseCheck case_check;
	for (idx_t i = 0; i < keys_unpacked.size(); i++) {
		if (base_expr) {
			auto max_type =
			    LogicalType::MaxLogicalType(input.context, base_expr->return_type, keys_unpacked[i]->return_type);
			case_check.when_expr = make_uniq<BoundComparisonExpression>(
			    ExpressionType::COMPARE_EQUAL, base_expr->Copy(),
			    BoundCastExpression::AddCastToType(input.context, std::move(keys_unpacked[i]), max_type));
		} else {
			case_check.when_expr =
			    BoundCastExpression::AddCastToType(input.context, std::move(keys_unpacked[i]), LogicalType::BOOLEAN);
		}
		auto then_type = values_unpacked[i]->return_type;
		if (!LogicalType::TryGetMaxLogicalType(input.context, function_data.return_type, then_type,
		                                       function_data.return_type)) {
			throw BinderException(
			    "Cannot mix values of type %s and %s in CASE expression - an explicit cast is required",
			    function_data.return_type.ToString(), then_type.ToString());
		}
		case_check.then_expr = std::move(values_unpacked[i]);
		result->case_checks.push_back(std::move(case_check));
	}
	if (default_expr) {
		result->else_expr = std::move(default_expr);
	} else {
		result->else_expr = BoundCastExpression::AddCastToType(
		    input.context, make_uniq<BoundConstantExpression>(Value()), function_data.return_type);
	}
	return std::move(result);
}

} // namespace

ScalarFunctionSet SwitchFun::GetFunctions() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");
	ScalarFunctionSet func_set;

	vector<vector<LogicalType>> function_variations = {{key_type, LogicalType::MAP(key_type, val_type)},
	                                                   {key_type, LogicalType::MAP(key_type, val_type), val_type},
	                                                   {LogicalType::MAP(key_type, val_type), val_type},
	                                                   {LogicalType::MAP(key_type, val_type)}};

	for (auto variation : function_variations) {
		auto switch_expression = ScalarFunction(variation, val_type, nullptr, SwitchBindReturnType, nullptr);
		switch_expression.SetBindExpressionCallback(SwitchBindExpression);
		func_set.AddFunction(std::move(switch_expression));
	}

	return func_set;
}
} // namespace duckdb
