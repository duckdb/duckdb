#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

static unique_ptr<FunctionData> ListSearchBind(BindScalarFunctionInput &input) {
	auto &arguments = input.GetArguments();
	auto &function = input.GetBoundFunction();
	auto &context = input.GetClientContext();
	auto list_type = arguments[0]->GetReturnType();
	if (list_type.id() != LogicalTypeId::LIST && list_type.id() != LogicalTypeId::ARRAY) {
		return nullptr;
	}
	auto &child_type =
	    list_type.id() == LogicalTypeId::LIST ? ListType::GetChildType(list_type) : ArrayType::GetChildType(list_type);
	auto is_struct = [](const LogicalType &type) {
		return StructType::IsStruct(type);
	};
	if (!TypeVisitor::Contains(child_type, is_struct) &&
	    !TypeVisitor::Contains(arguments[1]->GetReturnType(), is_struct)) {
		return nullptr;
	}
	auto comparison_type = LogicalType::MaxLogicalType(context, child_type, arguments[1]->GetReturnType());
	// Merge the element types before pushing collations so each field uses the collation from either argument.
	vector<LogicalType> types {LogicalType::LIST(comparison_type), comparison_type};
	for (idx_t i = 0; i < arguments.size(); i++) {
		arguments[i] = BoundCastExpression::AddCastToType(context, std::move(arguments[i]), types[i]);
		ExpressionBinder::PushCollation(context, arguments[i], types[i], CollationType::COMBINABLE_COLLATIONS);
		function.GetArguments()[i] = arguments[i]->GetReturnType();
	}
	return nullptr;
}

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

//! Whether the statistics prove that no list element can ever match the needle.
//! FIND_NULLS (list_position) also matches NULL elements against a NULL needle, whereas list_contains
//! never matches a NULL needle at all.
static bool ListSearchNeverMatches(optional_ptr<const BaseStatistics> list_stats,
                                   optional_ptr<const BaseStatistics> needle_stats, const bool find_nulls) {
	if (!list_stats || list_stats->GetStatsType() != StatisticsType::LIST_STATS) {
		return false;
	}
	if (!list_stats->CanHaveNoNull()) {
		// The list is always NULL, so there is nothing to search.
		return true;
	}

	auto &child = ListStats::GetChildStats(*list_stats);
	// A NULL needle finds a NULL element - only list_position looks for those.
	const bool null_needle_matches = find_nulls && child.CanHaveNull();

	if (!needle_stats) {
		// The needle is unknown, so it can only be ruled out if no element can match anything.
		return !null_needle_matches && !child.CanHaveNoNull();
	}
	if (!needle_stats->CanHaveNoNull()) {
		// The needle is always NULL.
		return !null_needle_matches;
	}
	if (null_needle_matches && needle_stats->CanHaveNull()) {
		// The needle can be NULL, in which case it matches any NULL element.
		return false;
	}
	if (!child.CanHaveNoNull()) {
		// Every element is NULL, and the needle we are searching for is not.
		return true;
	}
	if (child.GetType() != needle_stats->GetType()) {
		return false;
	}
	auto zonemap = StatisticsPropagator::PropagateComparison(child, *needle_stats, ExpressionType::COMPARE_EQUAL);
	return zonemap == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
	       zonemap == FilterPropagateResult::FILTER_FALSE_OR_NULL;
}

static FilterPropagateResult ListContainsFilterPrune(const FunctionStatisticsPruneInput &input) {
	auto list_stats = input.ChildStats(0);
	auto needle_stats = input.ChildStats(1);
	if (!ListSearchNeverMatches(list_stats, needle_stats, false)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	// list_contains is NULL if either side is NULL, and false otherwise.
	const bool can_have_null = list_stats->CanHaveNull() || !needle_stats || needle_stats->CanHaveNull();
	return can_have_null ? FilterPropagateResult::FILTER_FALSE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_FALSE;
}

static unique_ptr<BaseStatistics> ListPositionPropagateStats(ClientContext &, FunctionStatisticsInput &input) {
	if (input.child_stats.size() != 2) {
		return nullptr;
	}
	if (BaseStatistics::GetStatsType(input.expr.GetReturnType()) != StatisticsType::NUMERIC_STATS) {
		return nullptr;
	}
	if (!ListSearchNeverMatches(input.child_stats[0], input.child_stats[1], true)) {
		return nullptr;
	}
	// Needle cannot occur: list_position is NULL for every row.
	auto stats = NumericStats::CreateEmpty(input.expr.GetReturnType());
	stats.SetHasNullFast();
	return stats.ToUnique();
}

ScalarFunction ListContainsFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::LIST(LogicalType::TEMPLATE("T")), LogicalType::TEMPLATE("T")},
	                          LogicalType::BOOLEAN, ListSearchFunction<bool>);
	fun.SetCollationHandling(FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS);
	fun.SetBindCallback(ListSearchBind);
	fun.SetFilterPruneCallback(ListContainsFilterPrune);
	return fun;
}

ScalarFunction ListPositionFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::LIST(LogicalType::TEMPLATE("T")), LogicalType::TEMPLATE("T")},
	                          LogicalType::INTEGER, ListSearchFunction<int32_t, true>);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetCollationHandling(FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS);
	fun.SetBindCallback(ListSearchBind);
	fun.SetStatisticsCallback(ListPositionPropagateStats);
	return fun;
}

} // namespace duckdb
