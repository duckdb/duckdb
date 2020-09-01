//===----------------------------------------------------------------------===//
//
// benchmark/include/interpreted_benchmark.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "benchmark.hpp"

#include <unordered_map>
#include <unordered_set>

namespace duckdb {

//! Interpreted benchmarks read the benchmark from a file
class InterpretedBenchmark : public Benchmark {
public:
	InterpretedBenchmark(string full_path);

	void LoadBenchmark();
	//! Initialize the benchmark state
	unique_ptr<BenchmarkState> Initialize() override;
	//! Run the benchmark
	void Run(BenchmarkState *state) override;
	//! Cleanup the benchmark, called after each Run
	void Cleanup(BenchmarkState *state) override;
	//! Verify that the output of the benchmark was correct
	string Verify(BenchmarkState *state) override;

	string GetQuery() override {
		return run_query;
	}
	//! Interrupt the benchmark because of a timeout
	void Interrupt(BenchmarkState *state) override;
	//! Returns information about the benchmark
	string BenchmarkInfo() override;

	string GetLogOutput(BenchmarkState *state) override;
private:
	std::unordered_map<string, string> replacement_mapping;

	std::unordered_map<string, string> queries;
	string run_query;

	string benchmark_path;
	string data_cache;
	std::unordered_set<string> extensions;
	int64_t result_column_count = 0;
	vector<vector<string>> result_values;
};

} // namespace duckdb
