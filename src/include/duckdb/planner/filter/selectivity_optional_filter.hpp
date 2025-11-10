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
	ACTIVE, // ready and in use
	PAUSED_DUE_TO_ZONE_MAP_STATS,
	PAUSED_DUE_TO_HIGH_SELECTIVITY
};

class SelectivityOptionalFilter final : public TableFilter {

	struct SelectivityStats {
		atomic<idx_t> tuples_accepted;
		atomic<idx_t> tuples_processed;
		atomic<idx_t> vectors_processed;
		atomic<SelectivityOptionalFilterStatus> status;

		SelectivityStats()
		    : tuples_accepted(0), tuples_processed(0), vectors_processed(0),
		      status(SelectivityOptionalFilterStatus::ACTIVE) {
		}

		void Update(const idx_t accepted, const idx_t processed) {
			tuples_accepted += accepted;
			tuples_processed += processed;
			vectors_processed += 1;
		}

		double GetSelectivity() const {
			const idx_t processed = tuples_processed.load();
			if (processed == 0) {
				return 1.0;
			}
			return static_cast<double>(tuples_accepted.load()) / static_cast<double>(processed);
		}

		void SetStatus(const SelectivityOptionalFilterStatus new_status) {
			status.store(new_status);
		}

		unique_ptr<SelectivityStats> Copy() const {
			auto copy = make_uniq<SelectivityStats>();
			copy->tuples_accepted.store(tuples_accepted.load());
			copy->tuples_processed.store(tuples_processed.load());
			copy->vectors_processed.store(vectors_processed.load());
			copy->status.store(status.load());
			return copy;
		}
	};

private:
	float selectivity_threshold;
	idx_t n_vectors_to_check;

public:

	static constexpr float MIN_MAX_THRESHOLD = 0.75f;
	static constexpr idx_t MIN_MAX_CHECK_N = 40;

	static constexpr float BF_THRESHOLD = 0.25f;
	static constexpr idx_t BF_CHECK_N = 60;

	static constexpr auto TYPE = TableFilterType::SELECTIVITY_OPTIONAL_FILTER;
	unique_ptr<SelectivityStats> selectivity_stats;

	SelectivityOptionalFilter(unique_ptr<TableFilter> filter, float selectivity_threshold, idx_t n_vectors_to_check);
	unique_ptr<TableFilter> child_filter;

	void UpdateStats(const idx_t accepted, const idx_t processed) const {
		// check if we have already processed enough vectors
		if (selectivity_stats->vectors_processed.load() >= n_vectors_to_check) {
			return;
		}

		selectivity_stats->Update(accepted, processed);

		// pause the filter if we processed enough vectors and the selectivity is too high
		if (selectivity_stats->vectors_processed.load() >= n_vectors_to_check) {
			const double selectivity = selectivity_stats->GetSelectivity();
			if (selectivity >= selectivity_threshold) {
				selectivity_stats->SetStatus(SelectivityOptionalFilterStatus::PAUSED_DUE_TO_HIGH_SELECTIVITY);
			}
		}
	}

	bool IsActive() const {
		return selectivity_stats->status.load() == SelectivityOptionalFilterStatus::ACTIVE;
	}

public:
	string ToString(const string &column_name) const override;
	unique_ptr<TableFilter> Copy() const override;

	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
