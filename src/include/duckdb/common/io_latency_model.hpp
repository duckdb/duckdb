//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/io_latency_model.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

// Forward declaration.
class RandomEngine;

//! Generates latency values from a log-normal distribution to simulate I/O latency.
class IoLatencyModel {
public:
	IoLatencyModel(double mean_ms_p, double stddev_ms_p);

	//! Returns calculated latency
	double SampleLatency(RandomEngine &random);

private:
	double mean_ms;
	double stddev_ms;
	double log_mean;
	double log_stddev;
};

} // namespace duckdb
