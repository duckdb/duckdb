
#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include <random>

using namespace duckdb;
using namespace std;

#define GROUP_ROW_COUNT 10000000
#define GROUP_COUNT 5

DUCKDB_BENCHMARK(SimpleGroupByAggregate, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	// fixed seed random numbers
	std::uniform_int_distribution<> distribution(1, 10000);
	std::mt19937 gen;
	gen.seed(42);

	state->conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);");
	auto appender = state->conn.GetAppender("integers");
	// insert the elements into the database
	for (size_t i = 0; i < GROUP_ROW_COUNT; i++) {
		appender->begin_append_row();
		appender->append_int(i % GROUP_COUNT);
		appender->append_int(distribution(gen));
		appender->end_append_row();
	}
	state->conn.DestroyAppender();
}

virtual std::unique_ptr<DuckDBResult> RunQuery(DuckDBBenchmarkState *state) {
	return state->conn.Query("SELECT i, SUM(j) FROM integers GROUP BY i");
}

virtual std::string VerifyResult(DuckDBResult *result) {
	if (!result->GetSuccess()) {
		return result->GetErrorMessage();
	}
	if (result->size() != GROUP_COUNT) {
		return "Incorrect amount of rows in result";
	}
	return std::string();
}
FINISH_BENCHMARK(SimpleGroupByAggregate)
