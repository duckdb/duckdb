#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 2

//----------------------------- SUFFIX ----------------------------------------
DUCKDB_BENCHMARK(SuffixLineitemShort, "[suffix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
	return "SELECT l_shipinstruct FROM lineitem WHERE suffix(l_shipinstruct, 'NONE')";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Short Suffix LineItem";
}
FINISH_BENCHMARK(SuffixLineitemShort)

DUCKDB_BENCHMARK(SuffixLineitemMedium, "[suffix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
	return "SELECT l_shipinstruct FROM lineitem WHERE suffix(l_shipinstruct, 'CK RETURN')";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Medium Suffix LineItem";
}
FINISH_BENCHMARK(SuffixLineitemMedium)

DUCKDB_BENCHMARK(SuffixLineitemLong, "[suffix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override { // 25% of TPC-H SF 1
	return "SELECT l_shipinstruct FROM lineitem WHERE suffix(l_shipinstruct, 'LIVER IN PERSON')";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Long Suffix LineItem";
}
FINISH_BENCHMARK(SuffixLineitemLong)

//-------------------------------- LIKE ---------------------------------------
DUCKDB_BENCHMARK(SuffixLineitemLikeShort, "[suffix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_shipinstruct FROM lineitem WHERE l_shipinstruct LIKE '%NONE'";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Short Suffix LineItem LIKE";
}
FINISH_BENCHMARK(SuffixLineitemLikeShort)

DUCKDB_BENCHMARK(SuffixLineitemLikeMedium, "[suffix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_shipinstruct FROM lineitem WHERE l_shipinstruct LIKE '%CK RETURN'";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Medium Suffix LineItem LIKE";
}
FINISH_BENCHMARK(SuffixLineitemLikeMedium)

DUCKDB_BENCHMARK(SuffixLineitemLikeLong, "[suffix_tpch]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_shipinstruct FROM lineitem WHERE l_shipinstruct LIKE '%LIVER IN PERSON'";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "LongSuffix LineItem LIKE";
}
FINISH_BENCHMARK(SuffixLineitemLikeLong)
