#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "imdb.hpp"

using namespace duckdb;
using namespace std;


#define IMDB_QUERY_BODY(QNR)                                                                                          \
	virtual void Load(DuckDBBenchmarkState *state) {                                                                   \
		imdb::dbgen(state->db);                                                      \
	}                                                                                                                  \
	virtual string GetQuery() {                                                                                        \
		return imdb::get_query(QNR);                                                                \
	}                                                                                                                  \
	virtual string VerifyResult(QueryResult *result) {                                                                 \
		if (!result->success) {                                                                                        \
			return result->error;                                                                                      \
		}        /* FIXME */                                                                                                      \
		return ""; /*return compare_csv(*result, tpch::get_answer(SF, QNR),                                            \
		              true);  */                                                                                       \
	}                                                                                                                  \
	virtual string BenchmarkInfo() {                                                                                   \
		return StringUtil::Format("IMDB (JOB) Q%d SF%d: %s", QNR, imdb::get_query(QNR).c_str());                      \
	}

DUCKDB_BENCHMARK(IMDBQ001, "[imdb]")
IMDB_QUERY_BODY(1);
FINISH_BENCHMARK(IMDBQ001);

