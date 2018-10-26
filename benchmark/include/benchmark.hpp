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

#include <string>

namespace duckdb {

//! Base class for any state that has to be kept by a Benchmark
struct BenchmarkState {
	virtual ~BenchmarkState() {
	}
};

//! The base Benchmark class is a base class that is used to create and register
//! new benchmarks
class Benchmark {
	constexpr static int DEFAULT_NRUNS = 5;
	constexpr static int DEFAULT_TIMEOUT = 30;

  public:
	//! The name of the benchmark
	std::string name;
	//! The benchmark group this benchmark belongs to
	std::string group;

	Benchmark(std::string name, std::string group);

	//! Initialize the benchmark state
	virtual std::unique_ptr<BenchmarkState> Initialize() {
		return nullptr;
	}
	//! Run the benchmark
	virtual void Run(BenchmarkState *state) = 0;
	//! Verify that the output of the benchmark was correct
	virtual std::string Verify(BenchmarkState *state) = 0;
	//! Finalize the benchmark runner
	virtual void Finalize() {
	}
	//! Interrupt the benchmark because of a timeout
	virtual void Interrupt(BenchmarkState *state) = 0;

	//! Whether or not Initialize() should be called once for every run or just
	//! once
	virtual bool RequireReinit() {
		return false;
	}
	//! The amount of runs to do for this benchmark
	virtual int NRuns() {
		return DEFAULT_NRUNS;
	}
	//! The timeout for this benchmark (in seconds)
	virtual int Timeout() {
		return DEFAULT_TIMEOUT;
	}
};

} // namespace duckdb
