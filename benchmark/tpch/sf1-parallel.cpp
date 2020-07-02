#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 1

#define TPCH_QUERY_BODY_PARALLEL(QNR)                                                                                           \
	virtual void Load(DuckDBBenchmarkState *state) {                                                                   \
	    Connection con(state->db);                                                                                    \
	    con.Query("PRAGMA threads=4");                                                                                 \
		string cached_name = "tpch_sf" + to_string(SF);                                                                \
		if (!BenchmarkRunner::TryLoadDatabase(state->db, cached_name)) {                                               \
			tpch::dbgen(SF, state->db);                                                                                \
			BenchmarkRunner::SaveDatabase(state->db, cached_name);                                                     \
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

DUCKDB_BENCHMARK(Q01_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(1);
FINISH_BENCHMARK(Q01_PARALLEL)

DUCKDB_BENCHMARK(Q02_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(2);
FINISH_BENCHMARK(Q02_PARALLEL)

DUCKDB_BENCHMARK(Q03_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(3);
FINISH_BENCHMARK(Q03_PARALLEL)

DUCKDB_BENCHMARK(Q04_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(4);
FINISH_BENCHMARK(Q04_PARALLEL)

DUCKDB_BENCHMARK(Q05_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(5);
FINISH_BENCHMARK(Q05_PARALLEL)

DUCKDB_BENCHMARK(Q06_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(6);
FINISH_BENCHMARK(Q06_PARALLEL)

DUCKDB_BENCHMARK(Q07_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(7);
FINISH_BENCHMARK(Q07_PARALLEL)

DUCKDB_BENCHMARK(Q08_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(8);
FINISH_BENCHMARK(Q08_PARALLEL)

DUCKDB_BENCHMARK(Q09_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(9);
FINISH_BENCHMARK(Q09_PARALLEL)

DUCKDB_BENCHMARK(Q10_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(10);
FINISH_BENCHMARK(Q10_PARALLEL)

DUCKDB_BENCHMARK(Q11_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(11);
FINISH_BENCHMARK(Q11_PARALLEL)

DUCKDB_BENCHMARK(Q12_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(12);
FINISH_BENCHMARK(Q12_PARALLEL)

DUCKDB_BENCHMARK(Q13_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(13);
FINISH_BENCHMARK(Q13_PARALLEL)

DUCKDB_BENCHMARK(Q14_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(14);
FINISH_BENCHMARK(Q14_PARALLEL)

DUCKDB_BENCHMARK(Q15_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(15);
FINISH_BENCHMARK(Q15_PARALLEL)

DUCKDB_BENCHMARK(Q16_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(16);
FINISH_BENCHMARK(Q16_PARALLEL)

DUCKDB_BENCHMARK(Q17_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(17);
FINISH_BENCHMARK(Q17_PARALLEL)

DUCKDB_BENCHMARK(Q18_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(18);
FINISH_BENCHMARK(Q18_PARALLEL)

DUCKDB_BENCHMARK(Q19_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(19);
FINISH_BENCHMARK(Q19_PARALLEL)

DUCKDB_BENCHMARK(Q20_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(20);
FINISH_BENCHMARK(Q20_PARALLEL)

DUCKDB_BENCHMARK(Q21_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(21);
FINISH_BENCHMARK(Q21_PARALLEL)
DUCKDB_BENCHMARK(Q22_PARALLEL, "[tpch-sf1-parallel]")
TPCH_QUERY_BODY_PARALLEL(22);
FINISH_BENCHMARK(Q22_PARALLEL)
