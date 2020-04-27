#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 2

//------------------------- CONTAINS ------------------------------------------
DUCKDB_BENCHMARK(ContainsRegular, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 11.5% of TPC-H SF 1
	return "SELECT COUNT(*) FROM lineitem WHERE contains(l_comment, 'regular')";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Contains word 'regular' in the l_comment";
}
FINISH_BENCHMARK(ContainsRegular)

DUCKDB_BENCHMARK(ContainsAccording, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
	return "SELECT COUNT(*) FROM lineitem WHERE contains(l_comment, 'according')";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Contains word 'according' in the l_comment";
}
FINISH_BENCHMARK(ContainsAccording)

//-------------------------------- Contains LIKE ---------------------------------------
DUCKDB_BENCHMARK(ContainsRegularLIKE, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // ~19% of TPC-H SF 1
	return "SELECT COUNT(*) FROM lineitem WHERE l_comment LIKE '%regular%'";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Like word 'regular' in the l_comment";
}
FINISH_BENCHMARK(ContainsRegularLIKE)

DUCKDB_BENCHMARK(ContainsAccordingLIKE, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
	return "SELECT COUNT(*) FROM lineitem WHERE l_comment LIKE '%according%'";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Like word 'according' in the l_comment";
}
FINISH_BENCHMARK(ContainsAccordingLIKE)
