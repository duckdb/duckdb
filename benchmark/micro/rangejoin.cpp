
#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
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
	auto appender = state->conn.GetAppender("integers");
	// insert the elements into the database
	for (size_t i = 0; i < RANGEJOIN_COUNT; i++) {
		appender->begin_append_row();
		appender->append_int(distribution(gen));
		appender->append_int(distribution(gen));
		appender->end_append_row();
	}
	state->conn.DestroyAppender();
}

virtual std::string GetQuery() {
	return "SELECT * FROM integers a, integers b WHERE (a.i / 1000) > b.j;";
}

virtual std::string VerifyResult(DuckDBResult *result) {
	if (!result->GetSuccess()) {
		return result->GetErrorMessage();
	}
	return std::string();
}

virtual std::string BenchmarkInfo() {
	return StringUtil::Format(
	    "Runs the following query: \"SELECT * FROM "
	    "integers a, integers b WHERE (a.i / 1000) > b.j;\""
	    " on %d rows",
	    RANGEJOIN_COUNT);
}
FINISH_BENCHMARK(RangeJoin)
