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

//! Adaptive skip for redundant runtime filters.
struct SelectivityGate {
	enum class Status { ACTIVE, PAUSED_DUE_TO_HIGH_SELECTIVITY };

	SelectivityGate(idx_t n_vectors_to_check, float selectivity_threshold);

	bool SkipThisVector() { // records skipped pause vectors
		if (IsActive()) {
			return false;
		}
		Update(0, 0);
		return true;
	}
	void RecordActive(idx_t accepted, idx_t processed) { // feed active vector selectivity
		Update(accepted, processed);
	}

	void Update(idx_t accepted, idx_t processed);
	bool IsActive() const;
	double GetSelectivity() const;

	const idx_t n_vectors_to_check;
	const float selectivity_threshold;
	idx_t tuples_accepted;
	idx_t tuples_processed;
	idx_t vectors_processed;
	Status status;
	idx_t pause_multiplier; // grows while filter remains unselective
};

} // namespace duckdb
