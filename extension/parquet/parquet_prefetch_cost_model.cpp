#include "parquet_prefetch_cost_model.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

constexpr uint64_t PrefetchCostModel::GAP_MIN;
constexpr uint64_t PrefetchCostModel::GAP_MAX;
constexpr idx_t PrefetchCostModelState::MIN_SAMPLES;
constexpr double PrefetchCostModelState::ALPHA;

PrefetchCostModel PrefetchCostModel::LocalProfile() {
	return {1e-5, 2e9}; // 10 us, 2 GB/s -> ~20 KB
}

PrefetchCostModel PrefetchCostModel::RemoteProfile() {
	return {5e-2, 64e6}; // 50 ms, 64 MB/s -> ~3.2 MB
}

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
	// Keep the per-medium seed until the underlying handle has measured enough requests.
	if (estimate.sample_count < MIN_SAMPLES) {
		return;
	}
	const double latency = estimate.latency_seconds;
	const double bandwidth = estimate.bandwidth_bytes_per_s;
	// Ignore degenerate estimates (also catches NaN).
	if (!(latency > 0) || !(bandwidth > 0)) {
		return;
	}

	lock_guard<mutex> guard(lock);
	if (!measured_adopted) {
		model.latency_seconds = latency;
		model.bandwidth_bytes_per_s = bandwidth;
		measured_adopted = true;
	} else {

		model.latency_seconds = ALPHA * latency + (1.0 - ALPHA) * model.latency_seconds;
		model.bandwidth_bytes_per_s = ALPHA * bandwidth + (1.0 - ALPHA) * model.bandwidth_bytes_per_s;
	}
}

PrefetchCostModel PrefetchCostModelState::GetModel() const {
	lock_guard<mutex> guard(lock);
	return model;
}

} // namespace duckdb
