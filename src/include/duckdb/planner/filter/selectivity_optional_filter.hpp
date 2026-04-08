//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/selectivity_optional_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include <utility>

#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {
class BaseStatistics;
class ClientContext;
class Deserializer;
class Serializer;
class Vector;
struct SelectionVector;
struct UnifiedVectorFormat;

struct SelectivityOptionalFilterState final : public TableFilterState {
	enum class FilterStatus { ACTIVE, PAUSED_DUE_TO_HIGH_SELECTIVITY };

	struct SelectivityStats {
		SelectivityStats(idx_t n_vectors_to_check, float selectivity_threshold);

		void Update(idx_t accepted, idx_t processed);
		bool IsActive() const;
		double GetSelectivity() const;

		//! Configuration
		const idx_t n_vectors_to_check;
		const float selectivity_threshold;

		//! For computing selectivity stats
		idx_t tuples_accepted;
		idx_t tuples_processed;
		idx_t vectors_processed;

		//! Whether currently paused
		FilterStatus status;

		//! For increasing pause if filter is not selective enough
		idx_t pause_multiplier;
	};

	unique_ptr<TableFilterState> child_state;
	SelectivityStats stats;

	explicit SelectivityOptionalFilterState(unique_ptr<TableFilterState> child_state, const idx_t n_vectors_to_check,
	                                        const float selectivity_threshold)
	    : child_state(std::move(child_state)), stats(n_vectors_to_check, selectivity_threshold) {
	}
};

enum class SelectivityOptionalFilterType : uint8_t { MIN_MAX, BF, PHJ, PRF };

class SelectivityOptionalFilter final : public OptionalFilter {
public:
	float selectivity_threshold;
	idx_t n_vectors_to_check;

	SelectivityOptionalFilter(unique_ptr<TableFilter> filter, SelectivityOptionalFilterType type);
	SelectivityOptionalFilter(unique_ptr<TableFilter> filter, float selectivity_threshold, idx_t n_vectors_to_check);

public:
	unique_ptr<TableFilter> Copy() const override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
	void FiltersNullValues(const LogicalType &type, bool &filters_nulls, bool &filters_valid_values,
	                       TableFilterState &filter_state) const override;
	unique_ptr<TableFilterState> InitializeState(ClientContext &context) const override;
	idx_t FilterSelection(SelectionVector &sel, Vector &vector, UnifiedVectorFormat &vdata,
	                      TableFilterState &filter_state, idx_t scan_count, idx_t &approved_tuple_count) const override;

	bool IsOnlyForZoneMapFiltering() const override {
		return false;
	}
};
} // namespace duckdb
