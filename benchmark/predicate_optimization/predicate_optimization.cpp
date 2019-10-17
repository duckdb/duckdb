#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 1

DUCKDB_BENCHMARK(PredicateOptimizationModAndLike, "[predicate_optimization]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_orderkey, l_comment FROM lineitem WHERE l_orderkey % 5 == 0 AND l_comment LIKE '%str%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"SELECT l_orderkey, l_comment FROM lineitem WHERE l_orderkey % 5 == 0 AND l_comment LIKE '%str%'\"";
}
FINISH_BENCHMARK(PredicateOptimizationModAndLike)


DUCKDB_BENCHMARK(PredicateOptimizationLikeAndMod, "[predicate_optimization]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_orderkey, l_comment FROM lineitem WHERE l_comment LIKE '%str%' AND l_orderkey % 5 == 0;";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"SELECT l_orderkey, l_comment FROM lineitem WHERE l_orderkey % 5 == 0 AND l_comment LIKE '%str%'\"";
}
FINISH_BENCHMARK(PredicateOptimizationLikeAndMod)


DUCKDB_BENCHMARK(PredicateOptimizationMod, "[predicate_optimization]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_orderkey, l_comment FROM lineitem WHERE l_orderkey % 5 == 0;";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"SELECT l_orderkey, l_comment FROM lineitem WHERE l_orderkey % 5 == 0'\"";
}
FINISH_BENCHMARK(PredicateOptimizationMod)


DUCKDB_BENCHMARK(PredicateOptimizationLike, "[predicate_optimization]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_orderkey, l_comment FROM lineitem WHERE l_comment LIKE '%str%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"SELECT l_orderkey, l_comment FROM lineitem WHERE l_comment LIKE '%str%'\"";
}
FINISH_BENCHMARK(PredicateOptimizationLike)