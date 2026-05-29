#include "parquet_prefetch_cost_model.hpp"

#include "duckdb/common/helper.hpp"

namespace duckdb {

constexpr uint64_t PrefetchCostModel::GAP_MIN;
constexpr uint64_t PrefetchCostModel::GAP_MAX;
constexpr double NetworkPrefetchStats::ALPHA;

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

void NetworkPrefetchStats::RecordRead(idx_t bytes, double seconds) {
	if (seconds <= 0 || bytes == 0) {
		return;
	}
	const double latency = latency_seconds.load();
	const double bandwidth = bandwidth_bytes_per_s.load();
	const double expected_transfer_time = static_cast<double>(bytes) / bandwidth;
	// lets figure out if this read is dominated by either latency or bandwidth
	if (expected_transfer_time < latency) {
		// Latency-dominated sample
		const double observed_latency = seconds - expected_transfer_time;
		if (observed_latency > 0) {
			latency_seconds.store(WeightedAVG(latency, observed_latency));
		}
	} else {
		// Bandwidth-dominated sample
		const double transfer_time = seconds - latency;
		if (transfer_time > 0) {
			bandwidth_bytes_per_s.store(WeightedAVG(bandwidth, static_cast<double>(bytes) / transfer_time));
		}
	}
}

PrefetchCostModel NetworkPrefetchStats::GetModel() const {
	return {latency_seconds.load(), bandwidth_bytes_per_s.load()};
}

} // namespace duckdb
