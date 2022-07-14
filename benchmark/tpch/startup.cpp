#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "tpch-extension.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;

#define SF 1

#define TPCHStartup(QUERY)                                                                                             \
	string db_path = "duckdb_benchmark_db.db";                                                                         \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		DeleteDatabase(db_path);                                                                                       \
		{                                                                                                              \
			DuckDB db(db_path);                                                                                        \
			Connection con(db);                                                                                        \
			con.Query("CALL dbgen(sf=" + std::to_string(SF) + ")");                                                    \
		}                                                                                                              \
		{                                                                                                              \
			auto config = GetConfig();                                                                                 \
			config->options.checkpoint_wal_size = 0;                                                                   \
			DuckDB db(db_path, config.get());                                                                          \
		}                                                                                                              \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state) override {                                                          \
		auto config = GetConfig();                                                                                     \
		DuckDB db(db_path, config.get());                                                                              \
		Connection con(db);                                                                                            \
		state->result = con.Query(QUERY);                                                                              \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return string("Start a TPC-H SF1 database and run ") + QUERY + string(" in the database");                     \
	}

#define NormalConfig()                                                                                                 \
	unique_ptr<DBConfig> GetConfig() {                                                                                 \
		return make_unique<DBConfig>();                                                                                \
	}

DUCKDB_BENCHMARK(TPCHEmptyStartup, "[startup]")
TPCHStartup("SELECT * FROM lineitem WHERE 1=0") NormalConfig() string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
FINISH_BENCHMARK(TPCHEmptyStartup)

DUCKDB_BENCHMARK(TPCHCount, "[startup]")
TPCHStartup("SELECT COUNT(*) FROM lineitem") NormalConfig() string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
FINISH_BENCHMARK(TPCHCount)

DUCKDB_BENCHMARK(TPCHSimpleAggr, "[startup]")
TPCHStartup("SELECT SUM(l_extendedprice) FROM lineitem") NormalConfig() string
    VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
FINISH_BENCHMARK(TPCHSimpleAggr)

DUCKDB_BENCHMARK(TPCHQ1, "[startup]")
TPCHStartup("PRAGMA tpch(1)") NormalConfig() string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return compare_csv(*result, TPCHExtension::GetAnswer(SF, 1), true);
}
FINISH_BENCHMARK(TPCHQ1)
