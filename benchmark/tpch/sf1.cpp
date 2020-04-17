#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 1

#define TPCH_QUERY_BODY(QNR)                                                                                           \
	virtual void Load(DuckDBBenchmarkState *state) {                                                                   \
		if (!BenchmarkRunner::TryLoadDatabase(state->db, "tpch")) {                                                    \
			tpch::dbgen(SF, state->db);                                                                                \
			BenchmarkRunner::SaveDatabase(state->db, "tpch");                                                          \
		}                                                                                                              \
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

DUCKDB_BENCHMARK(Q01, "[tpch-sf1]")
TPCH_QUERY_BODY(1);
FINISH_BENCHMARK(Q01)

DUCKDB_BENCHMARK(Q02, "[tpch-sf1]")
TPCH_QUERY_BODY(2);
FINISH_BENCHMARK(Q02)

DUCKDB_BENCHMARK(Q03, "[tpch-sf1]")
TPCH_QUERY_BODY(3);
FINISH_BENCHMARK(Q03)

DUCKDB_BENCHMARK(Q04, "[tpch-sf1]")
TPCH_QUERY_BODY(4);
FINISH_BENCHMARK(Q04)

DUCKDB_BENCHMARK(Q05, "[tpch-sf1]")
TPCH_QUERY_BODY(5);
FINISH_BENCHMARK(Q05)

DUCKDB_BENCHMARK(Q06, "[tpch-sf1]")
TPCH_QUERY_BODY(6);
FINISH_BENCHMARK(Q06)

DUCKDB_BENCHMARK(Q07, "[tpch-sf1]")
TPCH_QUERY_BODY(7);
FINISH_BENCHMARK(Q07)

DUCKDB_BENCHMARK(Q08, "[tpch-sf1]")
TPCH_QUERY_BODY(8);
FINISH_BENCHMARK(Q08)

DUCKDB_BENCHMARK(Q09, "[tpch-sf1]")
TPCH_QUERY_BODY(9);
FINISH_BENCHMARK(Q09)

DUCKDB_BENCHMARK(Q10, "[tpch-sf1]")
TPCH_QUERY_BODY(10);
FINISH_BENCHMARK(Q10)

DUCKDB_BENCHMARK(Q11, "[tpch-sf1]")
TPCH_QUERY_BODY(11);
FINISH_BENCHMARK(Q11)

DUCKDB_BENCHMARK(Q12, "[tpch-sf1]")
TPCH_QUERY_BODY(12);
FINISH_BENCHMARK(Q12)

DUCKDB_BENCHMARK(Q13, "[tpch-sf1]")
TPCH_QUERY_BODY(13);
FINISH_BENCHMARK(Q13)

DUCKDB_BENCHMARK(Q14, "[tpch-sf1]")
TPCH_QUERY_BODY(14);
FINISH_BENCHMARK(Q14)

DUCKDB_BENCHMARK(Q15, "[tpch-sf1]")
TPCH_QUERY_BODY(15);
FINISH_BENCHMARK(Q15)

DUCKDB_BENCHMARK(Q16, "[tpch-sf1]")
TPCH_QUERY_BODY(16);
FINISH_BENCHMARK(Q16)

DUCKDB_BENCHMARK(Q17, "[tpch-sf1]")
TPCH_QUERY_BODY(17);
FINISH_BENCHMARK(Q17)

DUCKDB_BENCHMARK(Q18, "[tpch-sf1]")
TPCH_QUERY_BODY(18);
FINISH_BENCHMARK(Q18)

DUCKDB_BENCHMARK(Q19, "[tpch-sf1]")
TPCH_QUERY_BODY(19);
FINISH_BENCHMARK(Q19)

DUCKDB_BENCHMARK(Q20, "[tpch-sf1]")
TPCH_QUERY_BODY(20);
FINISH_BENCHMARK(Q20)

DUCKDB_BENCHMARK(Q21, "[tpch-sf1]")
TPCH_QUERY_BODY(21);
FINISH_BENCHMARK(Q21)
DUCKDB_BENCHMARK(Q22, "[tpch-sf1]")
TPCH_QUERY_BODY(22);
FINISH_BENCHMARK(Q22)
