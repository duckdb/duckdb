#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

DUCKDB_BENCHMARK(SELECT1Memory, "[storage]")
void Load(DuckDBBenchmarkState *state) override {
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	for (int32_t i = 0; i < 50000; i++) {
		state->conn.Query("SELECT 1");
	}
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Run the query \"SELECT 1\" 50K times in in-memory mode";
}
FINISH_BENCHMARK(SELECT1Memory)

DUCKDB_BENCHMARK(SELECT1Disk, "[storage]")
void Load(DuckDBBenchmarkState *state) override {
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	for (int32_t i = 0; i < 50000; i++) {
		state->conn.Query("SELECT 1");
	}
}
string VerifyResult(QueryResult *result) override {
	return string();
}
bool InMemory() override {
	return false;
}
string BenchmarkInfo() override {
	return "Run the query \"SELECT 1\" 50K times in in-memory mode";
}
FINISH_BENCHMARK(SELECT1Disk)
