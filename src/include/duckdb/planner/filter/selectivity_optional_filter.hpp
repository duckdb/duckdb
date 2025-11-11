//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/selectivity_optional_filter
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

enum class SelectivityOptionalFilterStatus : uint8_t {
	ACTIVE,
	PAUSED_DUE_TO_ZONE_MAP_STATS, // todo: use this to disable the filter for one zone map based on CheckStatistics
	PAUSED_DUE_TO_HIGH_SELECTIVITY
};

class SelectivityOptionalFilter final : public TableFilter {
public:
	static constexpr float MIN_MAX_THRESHOLD = 0.75f;
	static constexpr idx_t MIN_MAX_CHECK_N = 20;

	static constexpr float BF_THRESHOLD = 0.25f;
	static constexpr idx_t BF_CHECK_N = 30;

	static constexpr auto TYPE = TableFilterType::SELECTIVITY_OPTIONAL_FILTER;

	float selectivity_threshold;
	idx_t n_vectors_to_check;

	unique_ptr<TableFilter> child_filter;

	SelectivityOptionalFilter(unique_ptr<TableFilter> filter, float selectivity_threshold, idx_t n_vectors_to_check);

public:
	string ToString(const string &column_name) const override;
	unique_ptr<TableFilter> Copy() const override;

	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
