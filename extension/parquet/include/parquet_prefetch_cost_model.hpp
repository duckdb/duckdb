//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_prefetch_cost_model.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

#include <atomic>

namespace duckdb {

//! Cost model that decides how large a gap between two needed byte ranges may be
struct PrefetchCostModel {
	//! round-trip latency + request setup
	double latency_seconds;
	//! single-stream throughput, in bytes per second
	double bandwidth_bytes_per_s;

	//! Floor for the coalescing gap
	static constexpr uint64_t GAP_MIN = 1ULL << 12; //! 4 KiB
	//! Ceiling for the coalescing gap
	static constexpr uint64_t GAP_MAX = 32ULL << 20; //! 32 MiB

	//! Per-medium seeds used before any read has been measured
	static PrefetchCostModel LocalProfile();  //! ~20 KiB gap
	static PrefetchCostModel RemoteProfile(); //! ~3.2 MiB gap

	//! The coalescing gap implied by this model, clamped to [GAP_MIN, GAP_MAX]
	uint64_t GetColumnGapSize() const;
};

//! Measured Network Stats
class NetworkPrefetchStats {
public:
	explicit NetworkPrefetchStats(PrefetchCostModel seed)
	    : latency_seconds(seed.latency_seconds), bandwidth_bytes_per_s(seed.bandwidth_bytes_per_s) {
	}

	//! Record number of bytes and time (s) that take to get it
	void RecordRead(idx_t bytes, double seconds);

	//! Snapshot of the current estimate
	PrefetchCostModel GetModel() const;

private:
	static constexpr double ALPHA = 0.25;
	//! Running average that weights new samples at 0.25
	static double WeightedAVG(double prev, double sample) {
		return ALPHA * sample + (1.0 - ALPHA) * prev;
	}

	std::atomic<double> latency_seconds;
	std::atomic<double> bandwidth_bytes_per_s;
};

} // namespace duckdb
