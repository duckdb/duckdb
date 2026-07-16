#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

namespace duckdb {

namespace {

static bool SuffixFunction(const string_t &str, const string_t &suffix) {
	auto suffix_size = suffix.GetSize();
	auto str_size = str.GetSize();
	if (suffix_size > str_size) {
		return false;
	}

	auto suffix_data = suffix.GetData();
	auto str_data = str.GetData();
	auto suf_idx = UnsafeNumericCast<int32_t>(suffix_size) - 1;
	idx_t str_idx = str_size - 1;
	for (; suf_idx >= 0; --suf_idx, --str_idx) {
		if (suffix_data[suf_idx] != str_data[str_idx]) {
			return false;
		}
	}
	return true;
}

struct SuffixOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return SuffixFunction(left, right);
	}
};

FilterPropagateResult SuffixFilterPrune(const FunctionStatisticsPruneInput &input) {
	auto &children = input.function.GetChildren();
	if (children.size() != 2 || children[1]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	auto string_stats = input.ChildStats(0);
	if (!string_stats || string_stats->GetStatsType() != StatisticsType::STRING_STATS) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!string_stats->CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	auto &suffix_value = children[1]->Cast<BoundConstantExpression>().GetValue();
	if (suffix_value.IsNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	auto &suffix = StringValue::Get(suffix_value);
	if (StringStats::HasMaxStringLength(*string_stats) && StringStats::MaxStringLength(*string_stats) < suffix.size()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	if (StringStats::GetMinType(*string_stats) != StringStatsType::EXACT_STATS ||
	    StringStats::GetMaxType(*string_stats) != StringStatsType::EXACT_STATS) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto min = StringStats::Min(*string_stats);
	if (min != StringStats::Max(*string_stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!SuffixFunction(string_t(min), string_t(suffix))) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	return string_stats->CanHaveNull() ? FilterPropagateResult::NO_PRUNING_POSSIBLE
	                                   : FilterPropagateResult::FILTER_ALWAYS_TRUE;
}

} // namespace

ScalarFunction SuffixFun::GetFunction() {
	ScalarFunction function("suffix",                                     // name of the function
	                        {LogicalType::VARCHAR, LogicalType::VARCHAR}, // argument list
	                        LogicalType::BOOLEAN,                         // return type
	                        ScalarFunction::BinaryFunction<string_t, string_t, bool, SuffixOperator>);
	function.SetFilterPruneCallback(SuffixFilterPrune);
	return function;
}

} // namespace duckdb
