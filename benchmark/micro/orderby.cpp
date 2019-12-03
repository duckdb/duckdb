#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define ORDER_BY_ROW_COUNT 100000

DUCKDB_BENCHMARK(OrderBySingleColumn, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	// fixed seed random numbers
	std::uniform_int_distribution<> distribution(1, 10000);
	std::mt19937 gen;
	gen.seed(42);

	state->conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);");
	Appender appender(state->conn, "integers"); // insert the elements into the database
	for (size_t i = 0; i < ORDER_BY_ROW_COUNT; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(distribution(gen));
		appender.Append<int32_t>(distribution(gen));
		appender.EndRow();
	}
}

virtual string GetQuery() {
	return "SELECT * FROM integers ORDER BY i";
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != ORDER_BY_ROW_COUNT) {
		return "Incorrect amount of rows in result";
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"SELECT * FROM integers ORDER BY i\""
	                          " on %d rows",
	                          ORDER_BY_ROW_COUNT);
}
FINISH_BENCHMARK(OrderBySingleColumn)
