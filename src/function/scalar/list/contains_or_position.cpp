#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"

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

	auto &child = ListStats::GetChildStats(*list_stats);
	if (!list_stats->CanHaveNoNull() || !child.CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_FALSE_OR_NULL;
	}

	auto needle_stats = input.ChildStats(1);
	if (!needle_stats) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!needle_stats->CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_FALSE_OR_NULL;
	}
	if (child.GetType() != needle_stats->GetType()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	const bool can_have_null = list_stats->CanHaveNull() || needle_stats->CanHaveNull();
	const auto filter_false =
	    can_have_null ? FilterPropagateResult::FILTER_FALSE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_FALSE;

	auto zonemap = StatisticsPropagator::PropagateComparison(child, *needle_stats, ExpressionType::COMPARE_EQUAL);
	if (zonemap == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
	    zonemap == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
		return filter_false;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
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
