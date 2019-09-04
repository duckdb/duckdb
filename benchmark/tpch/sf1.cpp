#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 1

#define TPCH_QUERY_BODY(QNR)                                                                                           \
	virtual void Load(DuckDBBenchmarkState *state) {                                                                   \
		tpch::dbgen(SF, state->db);                                                                                    \
	}                                                                                                                  \
	virtual string GetQuery() {                                                                                        \
		return tpch::get_query(QNR);                                                                                   \
	}                                                                                                                  \
	virtual string VerifyResult(QueryResult *result) {                                                                 \
		if (!result->success) {                                                                                        \
			return result->error;                                                                                      \
		}                                                                                                              \
		return compare_csv(*result, tpch::get_answer(SF, QNR), true);                                                  \
	}                                                                                                                  \
	virtual string BenchmarkInfo() {                                                                                   \
		return StringUtil::Format("TPC-H Q%d SF%d: %s", QNR, SF, tpch::get_query(QNR).c_str());                        \
	}

DUCKDB_BENCHMARK(HQ01, "[tpch-sf1]")
TPCH_QUERY_BODY(1);
FINISH_BENCHMARK(HQ01)

DUCKDB_BENCHMARK(HQ02, "[tpch-sf1]")
TPCH_QUERY_BODY(2);
FINISH_BENCHMARK(HQ02)

DUCKDB_BENCHMARK(HQ03, "[tpch-sf1]")
TPCH_QUERY_BODY(3);
FINISH_BENCHMARK(HQ03)

DUCKDB_BENCHMARK(HQ04, "[tpch-sf1]")
TPCH_QUERY_BODY(4);
FINISH_BENCHMARK(HQ04)

DUCKDB_BENCHMARK(HQ05, "[tpch-sf1]")
TPCH_QUERY_BODY(5);
FINISH_BENCHMARK(HQ05)

DUCKDB_BENCHMARK(HQ06, "[tpch-sf1]")
TPCH_QUERY_BODY(6);
FINISH_BENCHMARK(HQ06)

DUCKDB_BENCHMARK(HQ07, "[tpch-sf1]")
TPCH_QUERY_BODY(7);
FINISH_BENCHMARK(HQ07)

DUCKDB_BENCHMARK(HQ08, "[tpch-sf1]")
TPCH_QUERY_BODY(8);
FINISH_BENCHMARK(HQ08)

DUCKDB_BENCHMARK(HQ09, "[tpch-sf1]")
TPCH_QUERY_BODY(9);
FINISH_BENCHMARK(HQ09)

DUCKDB_BENCHMARK(HQ10, "[tpch-sf1]")
TPCH_QUERY_BODY(10);
FINISH_BENCHMARK(HQ10)

DUCKDB_BENCHMARK(HQ11, "[tpch-sf1]")
TPCH_QUERY_BODY(11);
FINISH_BENCHMARK(HQ11)

DUCKDB_BENCHMARK(HQ12, "[tpch-sf1]")
TPCH_QUERY_BODY(12);
FINISH_BENCHMARK(HQ12)

DUCKDB_BENCHMARK(HQ13, "[tpch-sf1]")
TPCH_QUERY_BODY(13);
FINISH_BENCHMARK(HQ13)

DUCKDB_BENCHMARK(HQ14, "[tpch-sf1]")
TPCH_QUERY_BODY(14);
FINISH_BENCHMARK(HQ14)

DUCKDB_BENCHMARK(HQ15, "[tpch-sf1]")
TPCH_QUERY_BODY(15);
FINISH_BENCHMARK(HQ15)

DUCKDB_BENCHMARK(HQ16, "[tpch-sf1]")
TPCH_QUERY_BODY(16);
FINISH_BENCHMARK(HQ16)

DUCKDB_BENCHMARK(HQ17, "[tpch-sf1]")
TPCH_QUERY_BODY(17);
FINISH_BENCHMARK(HQ17)

DUCKDB_BENCHMARK(HQ18, "[tpch-sf1]")
TPCH_QUERY_BODY(18);
FINISH_BENCHMARK(HQ18)

DUCKDB_BENCHMARK(HQ19, "[tpch-sf1]")
TPCH_QUERY_BODY(19);
FINISH_BENCHMARK(HQ19)

DUCKDB_BENCHMARK(HQ20, "[tpch-sf1]")
TPCH_QUERY_BODY(20);
FINISH_BENCHMARK(HQ20)

DUCKDB_BENCHMARK(HQ21, "[tpch-sf1]")
TPCH_QUERY_BODY(21);
FINISH_BENCHMARK(HQ21)
DUCKDB_BENCHMARK(HQ22, "[tpch-sf1]")
TPCH_QUERY_BODY(22);
FINISH_BENCHMARK(HQ22)
