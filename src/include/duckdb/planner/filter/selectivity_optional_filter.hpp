//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/selectivity_optional_filter
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/filter/optional_filter.hpp"

namespace duckdb {

class SelectivityOptionalFilter final : public OptionalFilter {
public:
	static constexpr auto MIN_MAX_THRESHOLD = 0.75f;
	static constexpr idx_t MIN_MAX_CHECK_N = 30;

	static constexpr float BF_THRESHOLD = 0.25f;
	static constexpr idx_t BF_CHECK_N = 75;

	float selectivity_threshold;
	idx_t n_vectors_to_check;

	SelectivityOptionalFilter(unique_ptr<TableFilter> filter, float selectivity_threshold, idx_t n_vectors_to_check);

public:
	unique_ptr<TableFilter> Copy() const override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);

	unique_ptr<TableFilterState> InitializeState(ClientContext &context) const override;

	optional_ptr<SelectivityOptionalFilterState> ExecuteChildFilter(TableFilterState &filter_state) const override;
};
} // namespace duckdb
