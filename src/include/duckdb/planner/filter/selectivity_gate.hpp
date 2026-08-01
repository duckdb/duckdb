//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/selectivity_gate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Adaptive skip for a redundant runtime filter: after n_vectors_to_check vectors it pauses while selectivity
//! stays above the threshold, resuming periodically to re-check. Shared by the optional-filter executors and by
//! the pulled-up skip on ExpressionFilterState, so the check-skip-record dance lives in one place.
struct SelectivityGate {
	enum class Status { ACTIVE, PAUSED_DUE_TO_HIGH_SELECTIVITY };

	SelectivityGate(idx_t n_vectors_to_check, float selectivity_threshold);

	//! true if this vector may pass through untouched; records the paused vector when so.
	bool SkipThisVector() {
		if (IsActive()) {
			return false;
		}
		Update(0, 0);
		return true;
	}
	//! Feed (accepted, processed) of an active vector back into the running selectivity estimate.
	void RecordActive(idx_t accepted, idx_t processed) {
		Update(accepted, processed);
	}

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
	Status status;

	//! For increasing pause if filter is not selective enough
	idx_t pause_multiplier;
};

} // namespace duckdb
