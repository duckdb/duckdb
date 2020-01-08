#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define WINDOW_ROW_COUNT 100000

DUCKDB_BENCHMARK(Window, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	// fixed seed random numbers
	std::uniform_int_distribution<> distribution(1, 10000);
	std::mt19937 gen;
	gen.seed(42);

	state->conn.Query("CREATE TABLE integers(i INTEGER);");
	Appender appender(state->conn, "integers"); // insert the elements into the database
	for (size_t i = 0; i < WINDOW_ROW_COUNT; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(distribution(gen));
		appender.EndRow();
	}
}

virtual string GetQuery() {
	return "SELECT SUM(i) OVER(order by i rows between 1000 preceding and 1000 following) FROM integers";
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != WINDOW_ROW_COUNT) {
		return "Incorrect amount of rows in result";
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"%s\""
	                          " on %d rows",
	                          GetQuery().c_str(), WINDOW_ROW_COUNT);
}
FINISH_BENCHMARK(Window)
