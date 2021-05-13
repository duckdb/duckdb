#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

IsNullFilter::IsNullFilter() : TableFilter(TableFilterType::IS_NULL) {
}

FilterPropagateResult IsNullFilter::CheckStatistics(BaseStatistics &stats) {
	if (stats.CanHaveNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	} else {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
}

string IsNullFilter::ToString(const string &column_name) {
	return column_name + "IS NULL";
}

IsNotNullFilter::IsNotNullFilter() : TableFilter(TableFilterType::IS_NOT_NULL) {
}

FilterPropagateResult IsNotNullFilter::CheckStatistics(BaseStatistics &stats) {
	if (stats.CanHaveNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	} else {
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}
}

string IsNotNullFilter::ToString(const string &column_name) {
	return column_name + " IS NOT NULL";
}

} // namespace duckdb
