#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 1

DUCKDB_BENCHMARK(AdaptiveStringReorderingAND, "[expression_reordering]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT * FROM lineitem WHERE l_comment LIKE '%' AND l_comment LIKE '%s%' AND l_comment LIKE '%str%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute adaptive reordering query ...";
}
FINISH_BENCHMARK(AdaptiveStringReorderingAND)

DUCKDB_BENCHMARK(AdaptiveStringReorderingOR, "[expression_reordering]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT * FROM lineitem WHERE l_comment LIKE '%' OR l_comment LIKE '%s%' OR l_comment LIKE '%str%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute adaptive reordering query ...";
}
FINISH_BENCHMARK(AdaptiveStringReorderingOR)

DUCKDB_BENCHMARK(AdaptiveNumericReorderingAND, "[expression_reordering]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT * FROM lineitem WHERE l_quantity < 11 AND l_shipdate < 727272 AND l_receiptdate < 828282 AND l_tax "
	       "< 0.05;";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute adaptive reordering query ...";
}
FINISH_BENCHMARK(AdaptiveNumericReorderingAND)

DUCKDB_BENCHMARK(AdaptiveNumericReorderingOR, "[expression_reordering]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT * FROM lineitem WHERE l_quantity < 11 OR l_shipdate < 727272 OR l_receiptdate < 828282 OR l_tax < "
	       "0.05;";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute adaptive reordering query ...";
}
FINISH_BENCHMARK(AdaptiveNumericReorderingOR)

DUCKDB_BENCHMARK(AdaptiveMixedReorderingAND, "[expression_reordering]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT * FROM lineitem WHERE l_returnflag = 'R' AND l_orderkey > 5000 AND l_shipdate > 5;";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute adaptive reordering query ...";
}
FINISH_BENCHMARK(AdaptiveMixedReorderingAND)

DUCKDB_BENCHMARK(AdaptiveMixedReorderingOR, "[expression_reordering]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT * FROM lineitem WHERE l_returnflag = 'R' OR l_orderkey > 5000 OR l_shipdate > 5;";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute adaptive reordering query ...";
}
FINISH_BENCHMARK(AdaptiveMixedReorderingOR)
