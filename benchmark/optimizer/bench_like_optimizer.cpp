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
	return "Execute like optimizer query ...";
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
	return "Execute like query with out optimizer...";
}
FINISH_BENCHMARK(LikeWithoutOptimizer)

//-------------------------------- PREFIX -----------------------------------------------
DUCKDB_BENCHMARK(LikeOptimizerPrefix, "[bench_optimizer]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT count(*) FROM lineitem WHERE l_comment LIKE 'about%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute like optimizer query Prefix...";
}
FINISH_BENCHMARK(LikeOptimizerPrefix)


DUCKDB_BENCHMARK(LikeWithoutOptimizerPrefix, "[bench_optimizer]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT count(*) FROM lineitem WHERE l_comment LIKE 'about_%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute like query with out optimizer Prefix...";
}
FINISH_BENCHMARK(LikeWithoutOptimizerPrefix)

//-------------------------------- SUFFIX ----------------------------------------------
DUCKDB_BENCHMARK(LikeOptimizerSuffix, "[bench_optimizer]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT count(*) FROM lineitem WHERE l_comment LIKE '%.';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute like optimizer query Suffix...";
}
FINISH_BENCHMARK(LikeOptimizerSuffix)


DUCKDB_BENCHMARK(LikeWithoutOptimizerSuffix, "[bench_optimizer]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT count(*) FROM lineitem WHERE l_comment LIKE '_%.';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute like query with out optimizer Suffix...";
}
FINISH_BENCHMARK(LikeWithoutOptimizerSuffix)

//-------------------------------- CONTAINS ----------------------------------------------
DUCKDB_BENCHMARK(LikeOptimizerContains, "[bench_optimizer]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT count(*) FROM lineitem WHERE l_comment LIKE '%unusual%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute like optimizer query Contains...";
}
FINISH_BENCHMARK(LikeOptimizerContains)


DUCKDB_BENCHMARK(LikeWithoutOptimizerContains, "[bench_optimizer]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT count(*) FROM lineitem WHERE l_comment LIKE '_%unusual%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute like query with out optimizer Contains...";
}
FINISH_BENCHMARK(LikeWithoutOptimizerContains)

//-------------------------------- REORDERING ----------------------------------------------
DUCKDB_BENCHMARK(LikeOptimizerReordered, "[bench_optimizer]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT count(*) FROM lineitem WHERE l_comment LIKE '%.' AND l_comment LIKE '%unusual%' AND l_comment LIKE 'about%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute like optimizer query Reordered...";
}
FINISH_BENCHMARK(LikeOptimizerReordered)


DUCKDB_BENCHMARK(LikeWithoutOptimizerReordered, "[bench_optimizer]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT count(*) FROM lineitem WHERE l_comment LIKE '_%.' AND l_comment LIKE '_%unusual%' AND l_comment LIKE 'about_%';";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute like query with out optimizer Reordered...";
}
FINISH_BENCHMARK(LikeWithoutOptimizerReordered)
