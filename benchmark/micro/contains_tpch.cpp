#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 2

//------------------------- CONTAINS INSTR ------------------------------------
DUCKDB_BENCHMARK(ContainsRegularINSTR, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 11.5% of TPC-H SF 1
	return "SELECT COUNT(*) FROM lineitem WHERE contains_instr(l_comment, 'regular')";
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
FINISH_BENCHMARK(ContainsRegularINSTR)

DUCKDB_BENCHMARK(ContainsAccordingINSTR, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
    return "SELECT COUNT(*) FROM lineitem WHERE contains_instr(l_comment, 'according')";
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
FINISH_BENCHMARK(ContainsAccordingINSTR)

//------------------------- CONTAINS KMP ------------------------------------
DUCKDB_BENCHMARK(ContainsRegularKMP, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // ~19% of TPC-H SF 1
	return "SELECT COUNT(*) FROM lineitem WHERE contains_kmp(l_comment, 'regular')";
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
FINISH_BENCHMARK(ContainsRegularKMP)

DUCKDB_BENCHMARK(ContainsAccordingKMP, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
    return "SELECT COUNT(*) FROM lineitem WHERE contains_kmp(l_comment, 'according')";
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
FINISH_BENCHMARK(ContainsAccordingKMP)

//------------------------- CONTAINS BM ------------------------------------
DUCKDB_BENCHMARK(ContainsRegularBM, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // ~19% of TPC-H SF 1
	return "SELECT COUNT(*) FROM lineitem WHERE contains_bm(l_comment, 'regular')";
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
FINISH_BENCHMARK(ContainsRegularBM)

DUCKDB_BENCHMARK(ContainsAccordingBM, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
    return "SELECT COUNT(*) FROM lineitem WHERE contains_bm(l_comment, 'according')";
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
FINISH_BENCHMARK(ContainsAccordingBM)

//------------------------- CONTAINS STRSTR ------------------------------------
DUCKDB_BENCHMARK(ContainsRegularSTRSTR, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // ~19% of TPC-H SF 1
	return "SELECT COUNT(*) FROM lineitem WHERE contains_strstr(l_comment, 'regular')";
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
FINISH_BENCHMARK(ContainsRegularSTRSTR)

DUCKDB_BENCHMARK(ContainsAccordingSTRSTR, "[contains_tpch]")
void Load(DuckDBBenchmarkState *state) override {
    // load the data into the tpch schema
    tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
    return "SELECT COUNT(*) FROM lineitem WHERE contains_strstr(l_comment, 'according')";
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
FINISH_BENCHMARK(ContainsAccordingSTRSTR)

//-------------------------------- LIKE ---------------------------------------

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
