#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define ORDER_BY_ROW_COUNT 100000

DUCKDB_BENCHMARK(Window, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	// fixed seed random numbers
	std::uniform_int_distribution<> distribution(1, 10000);
	std::mt19937 gen;
	gen.seed(42);

	state->conn.Query("CREATE TABLE integers(i INTEGER);");
	Appender appender(state->db, DEFAULT_SCHEMA, "integers");
	// insert the elements into the database
	for (size_t i = 0; i < ORDER_BY_ROW_COUNT; i++) {
		appender.BeginRow();
		appender.AppendInteger(distribution(gen));
		appender.EndRow();
	}
	appender.Commit();
}

virtual string GetQuery() {
	return "SELECT SUM(i) OVER(order by i rows between 100 preceding and 100 following) FROM integers";
}

virtual string VerifyResult(DuckDBResult *result) {
	if (!result->GetSuccess()) {
		return result->GetErrorMessage();
	}
	if (result->size() != ORDER_BY_ROW_COUNT) {
		return "Incorrect amount of rows in result";
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"SELECT SUM(i) OVER(order by i rows between 10 preceding and "
	                          "10 following) FROM integers\""
	                          " on %d rows",
	                          ORDER_BY_ROW_COUNT);
}
FINISH_BENCHMARK(Window)
