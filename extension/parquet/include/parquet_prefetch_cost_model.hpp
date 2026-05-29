//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_prefetch_cost_model.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

//! Cost model that decides how large a gap between two needed byte ranges may be
struct PrefetchCostModel {
	//! round-trip latency + request setup
	double latency_seconds;
	//! single-stream throughput, in bytes per second
	double bandwidth_bytes_per_s;

	//! Floor for the coalescing gap
	static constexpr uint64_t GAP_MIN = 1ULL << 12; // 4 KiB
	//! Ceiling for the coalescing gap
	static constexpr uint64_t GAP_MAX = 32ULL << 20; // 32 MiB

	static PrefetchCostModel LocalProfile() {
		return {1e-5, 2e9}; // 10 us, 2 GB/s -> ~20 KB
	}
	static PrefetchCostModel RemoteProfile() {
		return {5e-2, 64e6}; // 50 ms, 64 MB/s -> ~3.2 MB
	}

	//! The coalescing gap implied by this model, clamped to [GAP_MIN, GAP_MAX]
	uint64_t CoalesceGap() const {
		const double bdp = latency_seconds * bandwidth_bytes_per_s;
		if (!(bdp > 0)) { // also catches NaN
			return GAP_MIN;
		}
		if (bdp >= static_cast<double>(GAP_MAX)) {
			return GAP_MAX;
		}
		return MaxValue<uint64_t>(GAP_MIN, static_cast<uint64_t>(bdp));
	}

	static uint64_t CoalesceGap(bool on_disk) {
		return (on_disk ? LocalProfile() : RemoteProfile()).CoalesceGap();
	}
};

} // namespace duckdb
