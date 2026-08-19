#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

namespace duckdb {

template <class RETURN_TYPE, bool FIND_NULLS = false>
static void ListSearchFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	if (result.GetType().id() == LogicalTypeId::SQLNULL) {
		ConstantVector::SetNull(result, count_t(input.size()));
		return;
	}

	auto target_count = input.size();
	const auto &input_list = input.data[0];
	const auto &list_child = ListVector::GetChild(input_list);
	const auto &target = input.data[1];

	ListSearchOp<RETURN_TYPE, FIND_NULLS>(input_list, list_child, target, result, target_count);
}

static bool TryGetConstantNeedle(const Expression &expr, Value &needle) {
	if (expr.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		needle = expr.Cast<BoundConstantExpression>().GetValue();
		return true;
	}
	if (BoundCastExpression::IsCast(expr)) {
		return TryGetConstantNeedle(BoundCastExpression::Child(expr.Cast<BoundFunctionExpression>()), needle);
	}
	return false;
}

static FilterPropagateResult ListContainsFilterPrune(const FunctionStatisticsPruneInput &input) {
	auto &children = input.function.GetChildren();
	if (children.size() != 2) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	auto list_stats = input.ChildStats(0);
	if (!list_stats || list_stats->GetStatsType() != StatisticsType::LIST_STATS) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!list_stats->CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	Value needle;
	if (!TryGetConstantNeedle(*children[1], needle)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (needle.IsNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	auto &child_stats = ListStats::GetChildStats(*list_stats);
	if (!child_stats.CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (needle.type() != child_stats.GetType()) {
		auto cast_needle = needle.DefaultTryCastAs(child_stats.GetType());
		if (!cast_needle) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		needle = std::move(*cast_needle);
	}

	FilterPropagateResult zonemap;
	switch (child_stats.GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		zonemap = NumericStats::CheckZonemap(child_stats, ExpressionType::COMPARE_EQUAL,
		                                     array_ptr<const Value>(&needle, 1));
		break;
	case StatisticsType::STRING_STATS:
		zonemap = StringStats::CheckZonemap(child_stats, ExpressionType::COMPARE_EQUAL,
		                                    array_ptr<const Value>(&needle, 1));
		break;
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	// Empty lists still do not contain the needle
	return zonemap == FilterPropagateResult::FILTER_ALWAYS_FALSE ? zonemap
	                                                             : FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

ScalarFunction ListContainsFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::LIST(LogicalType::TEMPLATE("T")), LogicalType::TEMPLATE("T")},
	                          LogicalType::BOOLEAN, ListSearchFunction<bool>);
	fun.SetCollationHandling(FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS);
	fun.SetFilterPruneCallback(ListContainsFilterPrune);
	return fun;
}

ScalarFunction ListPositionFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::LIST(LogicalType::TEMPLATE("T")), LogicalType::TEMPLATE("T")},
	                          LogicalType::INTEGER, ListSearchFunction<int32_t, true>);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetCollationHandling(FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS);
	return fun;
}

} // namespace duckdb
