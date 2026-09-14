#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {
namespace {
struct SwitchFunctionBindData : FunctionData {
	explicit SwitchFunctionBindData(const LogicalType &return_type_p, idx_t map_index_p)
	    : return_type(return_type_p), map_index(map_index_p) {
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

//! Which argument holds the cases map is a property of the overload that was resolved, not of the
//! argument types: a MAP-typed key argument makes the types ambiguous. Each variation binds its own index.
template <idx_t MAP_INDEX>
unique_ptr<FunctionData> SwitchBindReturnType(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	constexpr idx_t map_index = MAP_INDEX;
	D_ASSERT(map_index < arguments.size());
	auto &cases = arguments[map_index];
	if (cases->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		throw BinderException("SWITCH expected a constant map for the cases");
	}
	auto &func = cases->Cast<BoundFunctionExpression>();
	if (func.Function().GetName() != "map" || !cases->IsFoldable()) {
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
	if (list_function.Function().GetName() != "list_value") {
		throw BinderException("Expected a list function");
	}
	if (list_function.GetChildren().empty()) {
		throw BinderException("No values provided for SWITCH expression");
	}
	for (auto &list_child : list_function.GetChildrenMutable()) {
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
	if (BoundCastExpression::IsCast(*input.children[map_index])) {
		auto &cast_expr = input.children[map_index]->Cast<BoundFunctionExpression>();
		if (BoundCastExpression::Child(cast_expr).GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
			throw BinderException("SWITCH expected a map function for the cases");
		}
		cases = std::move(BoundCastExpression::ChildMutable(cast_expr));
	} else if (input.children[map_index]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		cases = std::move(input.children[map_index]);
	} else {
		throw BinderException("SWITCH expected a map function for the cases");
	}
	auto &cases_func = cases->Cast<BoundFunctionExpression>();
	D_ASSERT(cases_func.GetChildren().size() == 2);

	vector<unique_ptr<Expression>> keys_unpacked;
	vector<unique_ptr<Expression>> values_unpacked;
	ExtractConstantExprFromList(cases_func.GetChildrenMutable()[0], keys_unpacked);
	ExtractConstantExprFromList(cases_func.GetChildrenMutable()[1], values_unpacked);

	result->CaseChecksMutable().reserve(keys_unpacked.size());
	for (idx_t i = 0; i < keys_unpacked.size(); i++) {
		BoundCaseCheck case_check;
		if (base_expr) {
			auto max_type = LogicalType::MaxLogicalType(input.context, base_expr->GetReturnType(),
			                                            keys_unpacked[i]->GetReturnType());
			case_check.when_expr = BoundComparisonExpression::Create(
			    ExpressionType::COMPARE_EQUAL, base_expr->Copy(),
			    BoundCastExpression::AddCastToType(input.context, std::move(keys_unpacked[i]), max_type));
		} else {
			case_check.when_expr =
			    BoundCastExpression::AddCastToType(input.context, std::move(keys_unpacked[i]), LogicalType::BOOLEAN);
		}
		auto then_type = values_unpacked[i]->GetReturnType();
		if (!LogicalType::TryGetMaxLogicalType(input.context, function_data.return_type, then_type,
		                                       function_data.return_type)) {
			throw BinderException(
			    "Cannot mix values of type %s and %s in CASE expression - an explicit cast is required",
			    function_data.return_type.ToString(), then_type.ToString());
		}
		case_check.then_expr = std::move(values_unpacked[i]);
		result->CaseChecksMutable().push_back(std::move(case_check));
	}
	if (default_expr) {
		result->ElseMutable() = std::move(default_expr);
	} else {
		result->ElseMutable() = BoundCastExpression::AddCastToType(
		    input.context, make_uniq<BoundConstantExpression>(Value()), function_data.return_type);
	}
	return std::move(result);
}

} // namespace

ScalarFunctionSet SwitchFun::GetFunctions() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");
	ScalarFunctionSet func_set;

	// each variation is paired with the position of its MAP(K, V) parameter
	vector<pair<vector<LogicalType>, bind_scalar_function_t>> function_variations = {
	    {{key_type, LogicalType::MAP(key_type, val_type)}, SwitchBindReturnType<1>},
	    {{key_type, LogicalType::MAP(key_type, val_type), val_type}, SwitchBindReturnType<1>},
	    {{LogicalType::MAP(key_type, val_type), val_type}, SwitchBindReturnType<0>},
	    {{LogicalType::MAP(key_type, val_type)}, SwitchBindReturnType<0>}};

	for (const auto &variation : function_variations) {
		auto switch_expression = ScalarFunction(variation.first, val_type, nullptr, variation.second, nullptr);
		switch_expression.SetBindExpressionCallback(SwitchBindExpression);
		func_set.AddFunction(std::move(switch_expression));
	}

	return func_set;
}
} // namespace duckdb
