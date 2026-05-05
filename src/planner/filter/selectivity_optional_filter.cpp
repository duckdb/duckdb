//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/selectivity_optional_filter
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/selectivity_optional_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

#include "duckdb/planner/table_filter_state.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

SelectivityOptionalFilterState::SelectivityStats::SelectivityStats(const idx_t n_vectors_to_check,
                                                                   const float selectivity_threshold)
    : n_vectors_to_check(n_vectors_to_check), selectivity_threshold(selectivity_threshold), tuples_accepted(0),
      tuples_processed(0), vectors_processed(0), status(FilterStatus::ACTIVE), pause_multiplier(0) {
}

void SelectivityOptionalFilterState::SelectivityStats::Update(idx_t accepted, idx_t processed) {
	vectors_processed++;
	tuples_accepted += accepted;
	tuples_processed += processed;

	static constexpr idx_t VECTOR_PAUSE = 10;
	D_ASSERT(n_vectors_to_check < VECTOR_PAUSE);
	if (vectors_processed == MaxValue<idx_t>(pause_multiplier, 1) * VECTOR_PAUSE) {
		vectors_processed = 0;
		tuples_accepted = 0;
		tuples_processed = 0;
		status = FilterStatus::ACTIVE;
	} else if (vectors_processed >= n_vectors_to_check) {
		// pause the filter if we processed enough vectors and the selectivity is too high
		if (GetSelectivity() >= selectivity_threshold) {
			status = FilterStatus::PAUSED_DUE_TO_HIGH_SELECTIVITY;
			pause_multiplier++; // increase the pause duration
		} else {
			pause_multiplier = 0; // selective enough, reset the pause duration
		}
	}
}

bool SelectivityOptionalFilterState::SelectivityStats::IsActive() const {
	return status == FilterStatus::ACTIVE;
}
double SelectivityOptionalFilterState::SelectivityStats::GetSelectivity() const {
	if (tuples_processed == 0) {
		return 0.0;
	}
	return static_cast<double>(tuples_accepted) / static_cast<double>(tuples_processed);
}

LegacySelectivityOptionalFilter::LegacySelectivityOptionalFilter(unique_ptr<TableFilter> filter,
                                                                 float selectivity_threshold, idx_t n_vectors_to_check)
    : LegacyOptionalFilter(std::move(filter)), selectivity_threshold(selectivity_threshold),
      n_vectors_to_check(n_vectors_to_check) {
}

LegacySelectivityOptionalFilter::LegacySelectivityOptionalFilter(unique_ptr<TableFilter> filter,
                                                                 SelectivityOptionalFilterType type) {
	float threshold;
	idx_t vectors_to_check;
	GetThresholdAndVectorsToCheck(type, threshold, vectors_to_check);
	child_filter = std::move(filter);
	selectivity_threshold = threshold;
	n_vectors_to_check = vectors_to_check;
}

void LegacySelectivityOptionalFilter::Serialize(Serializer &serializer) const {
	LegacyOptionalFilter::Serialize(serializer);
	serializer.WritePropertyWithDefault<float>(201, "selectivity_threshold", selectivity_threshold);
	serializer.WritePropertyWithDefault<idx_t>(202, "n_vectors_to_check", n_vectors_to_check);
}

unique_ptr<TableFilter> LegacySelectivityOptionalFilter::Deserialize(Deserializer &deserializer) {
	auto result = unique_ptr<LegacySelectivityOptionalFilter>(new LegacySelectivityOptionalFilter(nullptr, 0.5f, 100));
	deserializer.ReadPropertyWithDefault<unique_ptr<TableFilter>>(200, "child_filter", result->child_filter);
	deserializer.ReadPropertyWithDefault<float>(201, "selectivity_threshold", result->selectivity_threshold);
	deserializer.ReadPropertyWithDefault<idx_t>(202, "n_vectors_to_check", result->n_vectors_to_check);
	return std::move(result);
}

unique_ptr<Expression> LegacySelectivityOptionalFilter::ToExpression(const Expression &column) const {
	auto child_expr = child_filter ? child_filter->ToExpression(column) : nullptr;
	return CreateSelectivityOptionalFilterExpression(std::move(child_expr), column.GetReturnType(),
	                                                 selectivity_threshold, n_vectors_to_check);
}

} // namespace duckdb
