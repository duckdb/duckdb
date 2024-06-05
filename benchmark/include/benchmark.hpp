//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// benchmark.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include "benchmark_configuration.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

//! Base class for any state that has to be kept by a Benchmark
struct BenchmarkState {
	virtual ~BenchmarkState() {
	}
};

//! The base Benchmark class is a base class that is used to create and register
//! new benchmarks
class Benchmark {
	constexpr static size_t DEFAULT_NRUNS = 5;
	Benchmark(Benchmark &) = delete;

public:
	//! The name of the benchmark
	string name;
	//! The benchmark group this benchmark belongs to
	string group;

	Benchmark(bool register_benchmark, string name, string group);

	//! Initialize the benchmark state
	virtual duckdb::unique_ptr<BenchmarkState> Initialize(BenchmarkConfiguration &config) {
		return nullptr;
	}
	//! Run the benchmark
	virtual void Run(BenchmarkState *state) = 0;
	//! Cleanup the benchmark, called after each Run
	virtual void Cleanup(BenchmarkState *state) = 0;
	//! Verify that the output of the benchmark was correct
	virtual string Verify(BenchmarkState *state) = 0;
	//! Finalize the benchmark runner
	virtual void Finalize() {
	}
	virtual string GetQuery() {
		return string();
	}
	virtual string DisplayName() {
		return name;
	}
	virtual string Group() {
		return group;
	}
	virtual string Subgroup() {
		return string();
	}
	//! Interrupt the benchmark because of a timeout
	virtual void Interrupt(BenchmarkState *state) = 0;
	//! Returns information about the benchmark
	virtual string BenchmarkInfo() = 0;

	string GetInfo() {
		return name + " - " + group + "\n" + BenchmarkInfo();
	}

	virtual string GetLogOutput(BenchmarkState *state) = 0;

	//! Whether or not Initialize() should be called once for every run or just
	//! once
	virtual bool RequireReinit() {
		return false;
	}
	//! The amount of runs to do for this benchmark
	virtual size_t NRuns() {
		return DEFAULT_NRUNS;
	}
	//! The timeout for this benchmark (in seconds)
	virtual optional_idx Timeout(const BenchmarkConfiguration &config) {
		return config.timeout_duration;
	}
};

} // namespace duckdb
