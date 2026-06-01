//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_prefetch_cost_model.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/mutex.hpp"

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

	//! Per-medium seeds used before any read has been measured
	static PrefetchCostModel LocalProfile();  //! ~20 KiB gap
	static PrefetchCostModel RemoteProfile(); //! ~3.2 MiB gap

	//! The coalescing gap implied by this model, clamped to [GAP_MIN, GAP_MAX]
	uint64_t GetColumnGapSize() const;
};

class PrefetchCostModelState {
public:
	explicit PrefetchCostModelState(PrefetchCostModel seed) : model(seed) {
	}

	//! Refine the model from a measured network throughput estimate.
	void RefineFromEstimate(const NetworkThroughputEstimate &estimate);

	//! Snapshot of the current model
	PrefetchCostModel GetModel() const;

private:
	//! Number of minimal measured network samples
	static constexpr idx_t MIN_SAMPLES = 4;
	//! Weight applied to an estimate
	static constexpr double ALPHA = 0.5;

	mutable mutex lock;
	PrefetchCostModel model;
	//! Whether we have switched from the seed to measured values
	bool measured_adopted = false;
};

} // namespace duckdb
