#include "duckdb/common/types/value.hpp"
#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
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

	if (child.GetStatsType() == StatisticsType::NUMERIC_STATS &&
	    needle_stats->GetStatsType() == StatisticsType::NUMERIC_STATS) {
		auto zonemap = StatisticsPropagator::PropagateComparison(child, *needle_stats, ExpressionType::COMPARE_EQUAL);
		if (zonemap == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
		    zonemap == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
			return filter_false;
		}
	} else if (child.GetStatsType() == StatisticsType::STRING_STATS &&
	           needle_stats->GetStatsType() == StatisticsType::STRING_STATS) {
		if (StringStats::HasMin(*needle_stats) &&
		    StringStats::GetMinType(*needle_stats) == StringStatsType::EXACT_STATS) {
			auto needle_min = StringStats::Min(*needle_stats);
			auto needle_value =
			    child.GetType().id() == LogicalTypeId::BLOB ? Value::BLOB_RAW(needle_min) : Value(needle_min);
			auto zonemap = StringStats::CheckZonemap(child, ExpressionType::COMPARE_GREATERTHANOREQUALTO,
			                                         array_ptr<const Value>(&needle_value, 1));
			if (zonemap == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				return filter_false;
			}
		}
		if (StringStats::HasMax(*needle_stats) &&
		    StringStats::GetMaxType(*needle_stats) == StringStatsType::EXACT_STATS) {
			auto needle_max = StringStats::Max(*needle_stats);
			auto needle_value =
			    child.GetType().id() == LogicalTypeId::BLOB ? Value::BLOB_RAW(needle_max) : Value(needle_max);
			auto zonemap = StringStats::CheckZonemap(child, ExpressionType::COMPARE_LESSTHANOREQUALTO,
			                                         array_ptr<const Value>(&needle_value, 1));
			if (zonemap == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				return filter_false;
			}
		}
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
