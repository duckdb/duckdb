#include "parquet_prefetch_cost_model.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

constexpr uint64_t PrefetchCostModel::GAP_MIN;
constexpr uint64_t PrefetchCostModel::GAP_MAX;
constexpr double PrefetchCostModelState::ALPHA;

uint64_t PrefetchCostModel::GetColumnGapSize() const {
	const double column_gap_size = latency_seconds * bandwidth_bytes_per_s;
	if (!(column_gap_size > 0)) { // also catches NaN
		return GAP_MIN;
	}
	if (column_gap_size >= static_cast<double>(GAP_MAX)) {
		return GAP_MAX;
	}
	return MaxValue<uint64_t>(GAP_MIN, static_cast<uint64_t>(column_gap_size));
}

void PrefetchCostModelState::RefineFromEstimate(const NetworkThroughputEstimate &estimate) {
	const double latency = estimate.latency_seconds;
	const double bandwidth = estimate.bandwidth_bytes_per_s;
	// Ignore degenerate estimates (also catches NaN).
	if (!(latency > 0) || !(bandwidth > 0)) {
		return;
	}

	if (!measured_adopted) {
		model.latency_seconds = latency;
		model.bandwidth_bytes_per_s = bandwidth;
		measured_adopted = true;
	} else {
		model.latency_seconds = ALPHA * latency + (1.0 - ALPHA) * model.latency_seconds;
		model.bandwidth_bytes_per_s = ALPHA * bandwidth + (1.0 - ALPHA) * model.bandwidth_bytes_per_s;
	}
}

bool PrefetchCostModelState::TryGetColumnGapSize(uint64_t &result) const {
	if (!measured_adopted) {
		return false;
	}
	result = model.GetColumnGapSize();
	return true;
}

} // namespace duckdb
