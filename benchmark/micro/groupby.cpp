#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "main/appender.hpp"

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
	Appender appender(state->db, DEFAULT_SCHEMA, "integers");
	// insert the elements into the database
	for (size_t i = 0; i < GROUP_ROW_COUNT; i++) {
		appender.BeginRow();
		appender.AppendInteger(i % GROUP_COUNT);
		appender.AppendInteger(distribution(gen));
		appender.EndRow();
	}
	appender.Commit();
}

virtual string GetQuery() {
	return "SELECT i, SUM(j) FROM integers GROUP BY i";
}

virtual string VerifyResult(DuckDBResult *result) {
	if (!result->GetSuccess()) {
		return result->GetErrorMessage();
	}
	if (result->size() != GROUP_COUNT) {
		return "Incorrect amount of rows in result";
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"SELECT i, SUM(j) "
	                          "FROM integers GROUP BY i\""
	                          " on %d rows with %d unique groups",
	                          GROUP_ROW_COUNT, GROUP_COUNT);
}
FINISH_BENCHMARK(SimpleGroupByAggregate)
