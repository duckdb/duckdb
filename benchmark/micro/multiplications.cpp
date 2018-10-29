
#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include <random>

using namespace duckdb;
using namespace std;

#define MULTIPLICATION_ROW_COUNT 10000000

DUCKDB_BENCHMARK(Multiplication, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	// fixed seed random numbers
	std::uniform_int_distribution<> distribution(1, 10000);
	std::mt19937 gen;
	gen.seed(42);

	state->conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);");
	auto appender = state->conn.GetAppender("integers");
	// insert the elements into the database
	for (size_t i = 0; i < MULTIPLICATION_ROW_COUNT; i++) {
		appender->begin_append_row();
		appender->append_int(distribution(gen));
		appender->append_int(distribution(gen));
		appender->end_append_row();
	}
	state->conn.DestroyAppender();
}

virtual std::unique_ptr<DuckDBResult> RunQuery(DuckDBBenchmarkState *state) {
	return state->conn.Query(
	    "SELECT (i * j) * (i * j) * (i * j) * (i * j) FROM integers");
}

virtual std::string VerifyResult(DuckDBResult *result) {
	if (!result->GetSuccess()) {
		return result->GetErrorMessage();
	}
	if (result->size() != MULTIPLICATION_ROW_COUNT) {
		return "Incorrect amount of rows in result";
	}
	return std::string();
}

virtual std::string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"SELECT (i * j) * (i "
	                          "* j) * (i * j) * (i * j) FROM integers\""
	                          " on %d rows",
	                          MULTIPLICATION_ROW_COUNT);
}
FINISH_BENCHMARK(Multiplication)
