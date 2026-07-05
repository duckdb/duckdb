//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_prefetch_cost_model.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

struct NetworkThroughputEstimate;

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

	//! The coalescing gap implied by this model, clamped to [GAP_MIN, GAP_MAX]
	uint64_t GetColumnGapSize() const;
};

class PrefetchCostModelState {
public:
	PrefetchCostModelState() = default;

	//! Refine the model from a measured throughput estimate
	void RefineFromEstimate(const NetworkThroughputEstimate &estimate);

	bool TryGetColumnGapSize(uint64_t &result) const;

private:
	//! Weight applied to an estimate
	static constexpr double ALPHA = 0.5;

	//! Measured cost model, zero until estimates arrive so coalescing starts at the gap floor
	PrefetchCostModel model = {0, 0};
	//! Whether we have switched from the seed to measured values
	bool measured_adopted = false;
};

} // namespace duckdb
