//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/selectivity_optional_filter
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/selectivity_optional_filter.hpp"
#include "duckdb/planner/table_filter_state.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/compression/compression.hpp"

namespace duckdb {

constexpr float SelectivityOptionalFilter::MIN_MAX_THRESHOLD;
constexpr idx_t SelectivityOptionalFilter::MIN_MAX_CHECK_N;

constexpr float SelectivityOptionalFilter::BF_THRESHOLD;
constexpr idx_t SelectivityOptionalFilter::BF_CHECK_N;

SelectivityOptionalFilter::SelectivityOptionalFilter(unique_ptr<TableFilter> filter, const float selectivity_threshold,
                                                     const idx_t n_vectors_to_check)
    : OptionalFilter(std::move(filter)), selectivity_threshold(selectivity_threshold),
      n_vectors_to_check(n_vectors_to_check) {
}

FilterPropagateResult SelectivityOptionalFilter::CheckStatistics(BaseStatistics &stats) const {
	// TODO: A potential optimization would be to pause the filter for this row group if the stats return always true,
	//		 but this needs to happen thread local, as other threads scan other row groups
	return child_filter->CheckStatistics(stats);
}

void SelectivityOptionalFilter::Serialize(Serializer &serializer) const {
	OptionalFilter::Serialize(serializer);
	serializer.WritePropertyWithDefault<float>(201, "selectivity_threshold", selectivity_threshold);
	serializer.WritePropertyWithDefault<idx_t>(202, "n_vectors_to_check", n_vectors_to_check);
}

unique_ptr<TableFilter> SelectivityOptionalFilter::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SelectivityOptionalFilter>(new SelectivityOptionalFilter(nullptr, 0.5f, 100));
	deserializer.ReadPropertyWithDefault<unique_ptr<TableFilter>>(200, "child_filter", result->child_filter);
	deserializer.ReadPropertyWithDefault<float>(201, "selectivity_threshold", result->selectivity_threshold);
	deserializer.ReadPropertyWithDefault<idx_t>(202, "n_vectors_to_check", result->n_vectors_to_check);
	return std::move(result);
}
void SelectivityOptionalFilter::FiltersNullValues(const LogicalType &type, bool &filters_nulls,
                                                  bool &filters_valid_values, TableFilterState &filter_state) const {
	const auto &state = filter_state.Cast<SelectivityOptionalFilterState>();
	return ConstantFun::FiltersNullValues(type, *this->child_filter, filters_nulls, filters_valid_values,
	                                      *state.child_state);
}
unique_ptr<TableFilterState> SelectivityOptionalFilter::InitializeState(ClientContext &context) const {
	D_ASSERT(child_filter);
	auto child_filter_state = TableFilterState::Initialize(context, *child_filter);
	return make_uniq<SelectivityOptionalFilterState>(std::move(child_filter_state), this->n_vectors_to_check,
	                                                 this->selectivity_threshold);
}

idx_t SelectivityOptionalFilter::FilterSelection(SelectionVector &sel, Vector &vector, UnifiedVectorFormat &vdata,
                                                 TableFilterState &filter_state, const idx_t scan_count,
                                                 idx_t &approved_tuple_count) const {
	auto &state = filter_state.Cast<SelectivityOptionalFilterState>();

	if (state.stats.IsActive()) {
		const idx_t approved_before = approved_tuple_count;
		const idx_t accepted_count = ColumnSegment::FilterSelection(
		    sel, vector, vdata, *child_filter, *state.child_state, scan_count, approved_tuple_count);

		state.stats.Update(accepted_count, approved_before);
		return accepted_count;
	}
	return scan_count;
}

unique_ptr<TableFilter> SelectivityOptionalFilter::Copy() const {
	auto copy = make_uniq<SelectivityOptionalFilter>(child_filter->Copy(), selectivity_threshold, n_vectors_to_check);
	return duckdb::unique_ptr_cast<SelectivityOptionalFilter, TableFilter>(std::move(copy));
}

} // namespace duckdb
