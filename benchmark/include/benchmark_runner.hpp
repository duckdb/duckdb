//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// benchmark_runner.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "benchmark.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/fstream.hpp"

namespace duckdb {

//! The benchmark runner class is responsible for running benchmarks
class BenchmarkRunner {
	BenchmarkRunner() {
	}

public:
	static BenchmarkRunner &GetInstance() {
		static BenchmarkRunner instance;
		return instance;
	}

	//! Register a benchmark in the Benchmark Runner, this is done automatically
	//! as long as the proper macro's are used
	static void RegisterBenchmark(Benchmark *benchmark);

	void Log(string message);
	void LogLine(string message);
	void LogResult(string message);
	void LogOutput(string message);

	void RunBenchmark(Benchmark *benchmark);
	void RunBenchmarks();

	vector<Benchmark *> benchmarks;
	ofstream out_file;
	ofstream log_file;
};

} // namespace duckdb
