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

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "benchmark.hpp"

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

	void Log(std::string message);
	void LogLine(std::string message);
	void LogResult(std::string message);
	void LogOutput(std::string message);

	void RunBenchmark(Benchmark *benchmark);
	void RunBenchmarks();

	std::vector<Benchmark *> benchmarks;
	std::ofstream out_file;
	std::ofstream log_file;
};

} // namespace duckdb
