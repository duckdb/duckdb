#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
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

static FilterPropagateResult ListContainsFilterPrune(const FunctionStatisticsPruneInput &input) {
	auto list_stats = input.ChildStats(0);
	if (!list_stats || list_stats->GetStatsType() != StatisticsType::LIST_STATS) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	const Expression *needle_expr = input.function.GetChildren()[1].get();
	while (BoundCastExpression::IsCast(*needle_expr)) {
		needle_expr = &BoundCastExpression::Child(needle_expr->Cast<BoundFunctionExpression>());
	}
	if (needle_expr->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto needle = needle_expr->Cast<BoundConstantExpression>().GetValue();

	auto &child = ListStats::GetChildStats(*list_stats);
	if (!list_stats->CanHaveNoNull() || !child.CanHaveNoNull() || needle.IsNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	auto cast = needle.DefaultTryCastAs(child.GetType());
	if (!cast) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	auto zonemap = FilterPropagateResult::NO_PRUNING_POSSIBLE;
	if (child.GetStatsType() == StatisticsType::NUMERIC_STATS) {
		zonemap = NumericStats::CheckZonemap(child, ExpressionType::COMPARE_EQUAL, array_ptr<const Value>(&*cast, 1));
	} else if (child.GetStatsType() == StatisticsType::STRING_STATS) {
		zonemap = StringStats::CheckZonemap(child, ExpressionType::COMPARE_EQUAL, array_ptr<const Value>(&*cast, 1));
	}
	return zonemap == FilterPropagateResult::FILTER_ALWAYS_FALSE ? zonemap : FilterPropagateResult::NO_PRUNING_POSSIBLE;
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
