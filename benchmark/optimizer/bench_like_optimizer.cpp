#include "benchmark_runner.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 2

DUCKDB_BENCHMARK(LikeOptimizer, "[bench_optimizer]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT count(*) FROM lineitem WHERE l_comment LIKE 'about%' AND l_comment LIKE '%.' AND l_comment LIKE '%unusual%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute like optimier query ...";
}
FINISH_BENCHMARK(LikeOptimizer)


DUCKDB_BENCHMARK(LikeWithoutOptimizer, "[bench_optimizer]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT count(*) FROM lineitem WHERE l_comment LIKE 'about_%' AND l_comment LIKE '_%.' AND l_comment LIKE '_%unusual%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute like query with out optimier...";
}
FINISH_BENCHMARK(LikeWithoutOptimizer)
