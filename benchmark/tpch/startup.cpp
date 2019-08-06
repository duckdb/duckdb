#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 1

#define TPCHStartup(QUERY)                                                                                             \
	string db_path = "duckdb_benchmark_db.db";                                                                         \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		DeleteDatabase(db_path);                                                                                       \
		{                                                                                                              \
			DuckDB db(db_path);                                                                                        \
			tpch::dbgen(SF, db);                                                                                       \
		}                                                                                                              \
		{                                                                                                              \
			auto config = GetConfig();                                                                                 \
			config->checkpoint_wal_size = 0;                                                                           \
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
TPCHStartup(tpch::get_query(1)) NormalConfig() string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return compare_csv(*result, tpch::get_answer(SF, 1), true);
}
FINISH_BENCHMARK(TPCHQ1)

#define DirectIOConfig()                                                                                               \
	unique_ptr<DBConfig> GetConfig() {                                                                                 \
		auto config = make_unique<DBConfig>();                                                                         \
		config->use_direct_io = true;                                                                                  \
		return config;                                                                                                 \
	}

DUCKDB_BENCHMARK(TPCHEmptyStartupDirectIO, "[startup]")
TPCHStartup("SELECT * FROM lineitem WHERE 1=0") DirectIOConfig() string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
FINISH_BENCHMARK(TPCHEmptyStartupDirectIO)

DUCKDB_BENCHMARK(TPCHCountDirectIO, "[startup]")
TPCHStartup("SELECT COUNT(*) FROM lineitem") DirectIOConfig() string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
FINISH_BENCHMARK(TPCHCountDirectIO)

DUCKDB_BENCHMARK(TPCHSimpleAggrDirectIO, "[startup]")
TPCHStartup("SELECT SUM(l_extendedprice) FROM lineitem") DirectIOConfig() string
    VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
FINISH_BENCHMARK(TPCHSimpleAggrDirectIO)

DUCKDB_BENCHMARK(TPCHQ1DirectIO, "[startup]")
TPCHStartup(tpch::get_query(1)) DirectIOConfig() string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return compare_csv(*result, tpch::get_answer(SF, 1), true);
}
FINISH_BENCHMARK(TPCHQ1DirectIO)
