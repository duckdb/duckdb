#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

namespace {

bool PrefixFunction(const string_t &str, const string_t &pattern) {
	auto str_length = str.GetSize();
	auto patt_length = pattern.GetSize();
	if (patt_length > str_length) {
		return false;
	}
	if (patt_length <= string_t::PREFIX_LENGTH) {
		// short prefix
		if (patt_length == 0) {
			// length = 0, return true
			return true;
		}

		// prefix early out
		const char *str_pref = str.GetPrefix();
		const char *patt_pref = pattern.GetPrefix();
		for (idx_t i = 0; i < patt_length; ++i) {
			if (str_pref[i] != patt_pref[i]) {
				return false;
			}
		}
		return true;
	} else {
		// prefix early out
		const char *str_pref = str.GetPrefix();
		const char *patt_pref = pattern.GetPrefix();
		for (idx_t i = 0; i < string_t::PREFIX_LENGTH; ++i) {
			if (str_pref[i] != patt_pref[i]) {
				// early out
				return false;
			}
		}
		// compare the rest of the prefix
		const char *str_data = str.GetData();
		const char *patt_data = pattern.GetData();
		D_ASSERT(patt_length <= str_length);
		for (idx_t i = string_t::PREFIX_LENGTH; i < patt_length; ++i) {
			if (str_data[i] != patt_data[i]) {
				return false;
			}
		}
		return true;
	}
}

struct PrefixOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return PrefixFunction(left, right);
	}
};

int8_t CompareStringStats(string_t input, string_t stats, StringStatsType type) {
	if (type == StringStatsType::TRUNCATED_STATS && input.GetSize() > stats.GetSize()) {
		return Comparator::Operation(string_t(input.GetData(), static_cast<uint32_t>(stats.GetSize())), stats);
	}
	return Comparator::Operation(input, stats);
}

// Find the next prefix of the given string
bool FindNextPrefix(string &prefix) {
	for (idx_t idx = prefix.size(); idx > 0; idx--) {
		auto c = static_cast<uint8_t>(prefix[idx - 1]);
		if (c < 0xFF) {
			prefix[idx - 1] = static_cast<char>(c + 1);
			prefix.resize(idx);
			return true;
		}
	}
	return false;
}

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
	if (!column_stats->CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	auto &constant = children[1]->Cast<BoundConstantExpression>().GetValue();
	if (constant.IsNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	auto prefix = StringValue::Get(constant);
	if (prefix.empty()) {
		return column_stats->CanHaveNull() ? FilterPropagateResult::NO_PRUNING_POSSIBLE
		                                   : FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}

	// Then check row group pruning with string stats min/max.
	if (StringStats::HasMaxStringLength(*column_stats) &&
	    StringStats::MaxStringLength(*column_stats) < prefix.size()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (!StringStats::HasMinMax(*column_stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	auto min = StringStats::Min(*column_stats);
	auto max = StringStats::Max(*column_stats);

	// prefix > max, always false
	if (CompareStringStats(string_t(prefix.c_str(), prefix.size()), string_t(max.c_str(), max.size()),
	                       StringStats::GetMaxType(*column_stats)) > 0) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	// next(prefix) <= min, always false
	auto upper_bound = prefix;
	if (FindNextPrefix(upper_bound)) {
		auto min_compare =
		    CompareStringStats(string_t(upper_bound.c_str(), upper_bound.size()), string_t(min.c_str(), min.size()),
		                       StringStats::GetMinType(*column_stats));
		if (min_compare < 0) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		if (min_compare == 0 && StringStats::GetMinType(*column_stats) == StringStatsType::EXACT_STATS) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

} // namespace

ScalarFunction PrefixFun::GetFunction() {
	ScalarFunction function("prefix",                                     // name of the function
	                        {LogicalType::VARCHAR, LogicalType::VARCHAR}, // argument list
	                        LogicalType::BOOLEAN,                         // return type
	                        ScalarFunction::BinaryFunction<string_t, string_t, bool, PrefixOperator>);
	function.SetFilterPruneCallback(PrefixFilterPrune);
	return function;
}

} // namespace duckdb
