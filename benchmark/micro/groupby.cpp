#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define GROUP_ROW_COUNT 10000000
#define GROUP_COUNT 5

DUCKDB_BENCHMARK(SimpleAggregate, "[aggregate]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE integers(i INTEGER);");
	Appender appender(state->conn, "integers"); // insert the elements into the database
	for (size_t i = 0; i < GROUP_ROW_COUNT; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i % GROUP_COUNT);
		appender.EndRow();
	}
}

virtual string GetQuery() {
	return "SELECT SUM(i) FROM integers";
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != 1) {
		return "Incorrect amount of rows in result";
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"SELECT SUM(i) "
	                          "FROM integers\""
	                          " on %d rows",
	                          GROUP_ROW_COUNT);
}
FINISH_BENCHMARK(SimpleAggregate)

DUCKDB_BENCHMARK(SimpleGroupByAggregate, "[aggregate]")
virtual void Load(DuckDBBenchmarkState *state) {
	// fixed seed random numbers
	std::uniform_int_distribution<> distribution(1, 10000);
	std::mt19937 gen;
	gen.seed(42);

	state->conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);");
	Appender appender(state->conn, "integers"); // insert the elements into the database
	for (size_t i = 0; i < GROUP_ROW_COUNT; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i % GROUP_COUNT);
		appender.Append<int32_t>(distribution(gen));
		appender.EndRow();
	}
}

virtual string GetQuery() {
	return "SELECT i, SUM(j) FROM integers GROUP BY i";
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != GROUP_COUNT) {
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
