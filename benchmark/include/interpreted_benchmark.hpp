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
	unique_ptr<BenchmarkState> Initialize(BenchmarkConfiguration &config) override;
	//! Run the benchmark
	void Run(BenchmarkState *state) override;
	//! Cleanup the benchmark, called after each Run
	void Cleanup(BenchmarkState *state) override;
	//! Verify that the output of the benchmark was correct
	string Verify(BenchmarkState *state) override;

	string GetQuery() override;
	//! Interrupt the benchmark because of a timeout
	void Interrupt(BenchmarkState *state) override;
	//! Returns information about the benchmark
	string BenchmarkInfo() override;

	string GetLogOutput(BenchmarkState *state) override;

	string DisplayName() override;
	string Group() override;
	string Subgroup() override;

	string GetDatabasePath();

	bool InMemory() {
		return in_memory;
	}

	bool RequireReinit() override {
		return require_reinit;
	}

private:
	bool is_loaded = false;
	std::unordered_map<string, string> replacement_mapping;

	std::unordered_map<string, string> queries;
	string run_query;

	string benchmark_path;
	string data_cache;
	string db_path = "";
	std::unordered_set<string> extensions;
	int64_t result_column_count = 0;
	vector<vector<string>> result_values;

	string display_name;
	string display_group;
	string subgroup;

	bool in_memory = true;
	bool require_reinit = false;
};

} // namespace duckdb
