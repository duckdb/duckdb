//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/selectivity_optional_filter
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/selectivity_optional_filter.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

SelectivityOptionalFilter::SelectivityOptionalFilter(unique_ptr<TableFilter> filter,
                                                     const float selectivity_threshold_p,
                                                     const idx_t n_vectors_to_check_p)
    : TableFilter(TYPE), selectivity_threshold(selectivity_threshold_p), n_vectors_to_check(n_vectors_to_check_p),
      selectivity_stats(make_uniq<SelectivityStats>()), child_filter(std::move(filter)) {
}

FilterPropagateResult SelectivityOptionalFilter::CheckStatistics(BaseStatistics &stats) const {
	const auto child_result = child_filter->CheckStatistics(stats);

	if (selectivity_stats->status.load() != SelectivityOptionalFilterStatus::PAUSED_DUE_TO_HIGH_SELECTIVITY) {
		if (child_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
			selectivity_stats->SetStatus(SelectivityOptionalFilterStatus::PAUSED_DUE_TO_ZONE_MAP_STATS);
		} else if (child_result == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
			selectivity_stats->SetStatus(SelectivityOptionalFilterStatus::ACTIVE);
		}
	}

	return child_result;
}

unique_ptr<Expression> SelectivityOptionalFilter::ToExpression(const Expression &column) const {
	return child_filter->ToExpression(column);
}

void SelectivityOptionalFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<TableFilter>>(200, "child_filter", child_filter);
}

unique_ptr<TableFilter> SelectivityOptionalFilter::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SelectivityOptionalFilter>(new SelectivityOptionalFilter(nullptr, 0.5f, 100));
	deserializer.ReadPropertyWithDefault<unique_ptr<TableFilter>>(200, "child_filter", result->child_filter);
	return std::move(result);
}

string SelectivityOptionalFilter::ToString(const string &column_name) const {
	const auto active_string = EnumUtil::ToString(selectivity_stats->status.load());
	const auto child_string = child_filter ? child_filter->ToString(column_name) : "NULL";
	return child_string + " [" + active_string + "]";
}

unique_ptr<TableFilter> SelectivityOptionalFilter::Copy() const {
	auto copy = make_uniq<SelectivityOptionalFilter>(child_filter->Copy(), selectivity_threshold, n_vectors_to_check);
	copy->selectivity_stats = selectivity_stats->Copy();
	return duckdb::unique_ptr_cast<SelectivityOptionalFilter, TableFilter>(std::move(copy));
}

} // namespace duckdb
