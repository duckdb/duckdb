#include "duckdb/function/scalar/string_common.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

namespace duckdb {

FilterPropagateResult PrefixFilterPrune(const FunctionStatisticsPruneInput &input) {
	auto &children = input.function.GetChildren();

	// First check whether it's possible to prune completely.
	if (children.size() != 2 || children[1]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	auto column_stats = input.ChildStats(0);
	if (!column_stats || column_stats->GetStatsType() != StatisticsType::STRING_STATS) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	// If all rows are null, always false
	if (!column_stats->CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	// If the constant is null, always false
	auto &constant = children[1]->Cast<BoundConstantExpression>().GetValue();
	if (constant.IsNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	// Handle empty prefix
	auto prefix = StringValue::Get(constant);
	if (prefix.empty()) {
		return column_stats->CanHaveNull() ? FilterPropagateResult::NO_PRUNING_POSSIBLE
		                                   : FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}

	// Then check row group pruning with string stats min/max.
	if (StringStats::HasMaxStringLength(*column_stats) && StringStats::MaxStringLength(*column_stats) < prefix.size()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (!StringStats::HasMinMax(*column_stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	const auto min = StringStats::Min(*column_stats);
	const auto max = StringStats::Max(*column_stats);

	// prefix > max, always false
	if (StringStats::CompareStringStats(string_t(prefix.c_str(), NumericCast<uint32_t>(prefix.size())),
	                                    string_t(max.c_str(), NumericCast<uint32_t>(max.size())),
	                                    StringStats::GetMaxType(*column_stats)) > 0) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	// next(prefix) <= min, always false
	auto upper_bound = prefix;
	if (StringUtil::FindNextPrefix(upper_bound)) {
		const auto min_compare = StringStats::CompareStringStats(
		    string_t(upper_bound.c_str(), NumericCast<uint32_t>(upper_bound.size())),
		    string_t(min.c_str(), NumericCast<uint32_t>(min.size())), StringStats::GetMinType(*column_stats));
		if (min_compare < 0) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		if (min_compare == 0 && StringStats::GetMinType(*column_stats) == StringStatsType::EXACT_STATS) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	}
	// Cases where min and max both start with the prefix.
	if (min.size() >= prefix.size() && max.size() >= prefix.size() &&
	    memcmp(min.c_str(), prefix.c_str(), prefix.size()) == 0 &&
	    memcmp(max.c_str(), prefix.c_str(), prefix.size()) == 0) {
		// NULL values produce NULL rather than true, so they prevent an always-true result
		return column_stats->CanHaveNull() ? FilterPropagateResult::NO_PRUNING_POSSIBLE
		                                   : FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

} // namespace duckdb
