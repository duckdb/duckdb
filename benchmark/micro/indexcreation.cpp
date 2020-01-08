#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define ROW_COUNT 100000000
#define UPPERBOUND 100000000
#define SUCCESS 0

DUCKDB_BENCHMARK(IndexCreationART, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE integers(i INTEGER);");
	Appender appender(state->conn, "integers"); // insert the elements into the database
	for (size_t i = 0; i < ROW_COUNT; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(rand() % UPPERBOUND);
		appender.EndRow();
	}
}

virtual string GetQuery() {
	return "CREATE INDEX i_index ON integers using art(i)";
}

virtual void Cleanup(DuckDBBenchmarkState *state) {
	state->conn.Query("DROP INDEX i_index;");
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != 1) {
		return "Incorrect amount of rows in result";
	}
	if (materialized.names.size() != 1) {
		return "Incorrect amount of columns";
	}
	if (materialized.GetValue<int32_t>(0, 0) != SUCCESS) {
		return "Incorrect result returned, expected " + to_string(SUCCESS);
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Creates an ART Index on a Uniform Random Column");
}

FINISH_BENCHMARK(IndexCreationART)
