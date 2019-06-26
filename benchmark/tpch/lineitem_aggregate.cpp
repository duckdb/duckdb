#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 1

DUCKDB_BENCHMARK(LineitemSimpleAggregate, "[aggregate]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT SUM(l_quantity) FROM lineitem";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"SELECT SUM(l_quantity) FROM lineitem\" on SF1";
}
FINISH_BENCHMARK(LineitemSimpleAggregate)

DUCKDB_BENCHMARK(LineitemGroupAggregate, "[aggregate]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_quantity, SUM(l_quantity) FROM lineitem GROUP BY l_quantity";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"SELECT l_quantity, SUM(l_quantity) FROM lineitem GROUP BY l_quantity\" on SF1";
}
FINISH_BENCHMARK(LineitemGroupAggregate)

DUCKDB_BENCHMARK(LineitemGroupStringAggregate, "[aggregate]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT SUM(l_quantity) FROM lineitem GROUP BY l_returnflag";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"SELECT SUM(l_quantity) FROM lineitem GROUP BY l_returnflag\" on SF1";
}
FINISH_BENCHMARK(LineitemGroupStringAggregate)
