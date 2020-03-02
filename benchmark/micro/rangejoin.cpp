#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define RANGEJOIN_COUNT 100000

DUCKDB_BENCHMARK(RangeJoin, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	// fixed seed random numbers
	std::uniform_int_distribution<> distribution(1, 10000);
	std::mt19937 gen;
	gen.seed(42);

	state->conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);");
	Appender appender(state->conn, "integers"); // insert the elements into the database
	for (size_t i = 0; i < RANGEJOIN_COUNT; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(distribution(gen));
		appender.Append<int32_t>(distribution(gen));
		appender.EndRow();
	}
}

virtual string GetQuery() {
	return "SELECT * FROM integers a, integers b WHERE (a.i / 1000) > b.j;";
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"SELECT * FROM "
	                          "integers a, integers b WHERE (a.i / 1000) > b.j;\""
	                          " on %d rows",
	                          RANGEJOIN_COUNT);
}
FINISH_BENCHMARK(RangeJoin)
