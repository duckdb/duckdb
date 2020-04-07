#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 2

//----------------------------- PREFIX ----------------------------------------
DUCKDB_BENCHMARK(PrefixLineitem, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
	return "SELECT l_shipinstruct FROM lineitem WHERE prefix(l_shipinstruct, 'NONE')";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Prefix early out LineItem";
}
FINISH_BENCHMARK(PrefixLineitem)

DUCKDB_BENCHMARK(PrefixLineitemInlined, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
	return "SELECT l_shipinstruct FROM lineitem WHERE prefix(l_shipinstruct, 'COLLECT')";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemInlined)

DUCKDB_BENCHMARK(PrefixLineitemPointer, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
	return "SELECT l_shipinstruct FROM lineitem WHERE prefix(l_shipinstruct, 'DELIVER IN PERS')";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Prefix inlined LineItem";
}
FINISH_BENCHMARK(PrefixLineitemPointer)

//-------------------------------- LIKE ---------------------------------------
DUCKDB_BENCHMARK(PrefixLineitemLike, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_shipinstruct FROM lineitem WHERE l_shipinstruct LIKE 'NONE%'";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Prefix early out LineItem LIKE";
}
FINISH_BENCHMARK(PrefixLineitemLike)

DUCKDB_BENCHMARK(PrefixLineitemInlinedLike, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_shipinstruct FROM lineitem WHERE l_shipinstruct LIKE 'COLLECT%'";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Prefix inlined LineItem LIKE";
}
FINISH_BENCHMARK(PrefixLineitemInlinedLike)

DUCKDB_BENCHMARK(PrefixLineitemPointerLike, "[prefix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_shipinstruct FROM lineitem WHERE l_shipinstruct LIKE 'DELIVER IN PERS%'";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Prefix inlined LineItem LIKE";
}
FINISH_BENCHMARK(PrefixLineitemPointerLike)
