#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

IsNullFilter::IsNullFilter() : TableFilter(TableFilterType::IS_NULL) {
}

unique_ptr<TableFilter> IsNullFilter::Copy() {
	return make_uniq<IsNullFilter>();
}

FilterPropagateResult IsNullFilter::CheckStatistics(BaseStatistics &stats) {
	if (!stats.CanHaveNull()) {
		// no null values are possible: always false
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (!stats.CanHaveNoNull()) {
		// no non-null values are possible: always true
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

string IsNullFilter::ToString(const string &column_name) {
	return column_name + "IS NULL";
}

IsNotNullFilter::IsNotNullFilter() : TableFilter(TableFilterType::IS_NOT_NULL) {
}

unique_ptr<TableFilter> IsNotNullFilter::Copy() {
	return make_uniq<IsNotNullFilter>();
}

FilterPropagateResult IsNotNullFilter::CheckStatistics(BaseStatistics &stats) {
	if (!stats.CanHaveNoNull()) {
		// no non-null values are possible: always false
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (!stats.CanHaveNull()) {
		// no null values are possible: always true
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

string IsNotNullFilter::ToString(const string &column_name) {
	return column_name + " IS NOT NULL";
}

void IsNotNullFilter::Serialize(FieldWriter &writer) const {
}

unique_ptr<TableFilter> IsNotNullFilter::Deserialize(FieldReader &source) {
	return make_uniq<IsNotNullFilter>();
}

void IsNullFilter::Serialize(FieldWriter &writer) const {
}

unique_ptr<TableFilter> IsNullFilter::Deserialize(FieldReader &source) {
	return make_uniq<IsNullFilter>();
}

} // namespace duckdb
