#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define RANGE_QUERY_ROW_COUNT pow(10, 7)
#define SUM_RESULT 100

DUCKDB_BENCHMARK(DataSkipping, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);");
	Appender appender(state->conn, "integers"); // insert the elements into the database
	for (size_t i = 0; i < RANGE_QUERY_ROW_COUNT; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.Append<int32_t>(i);
		appender.EndRow();
	}
	appender.Close();
}
virtual std::string GetQuery() {
	return "SELECT sum(j) FROM integers WHERE i = 100";
}

virtual std::string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != 1) {
		return "Incorrect amount of rows in result";
	}
	if (result->names.size() != 1) {
		return "Incorrect amount of columns";
	}
	if (materialized.GetValue<int64_t>(0, 0) != SUM_RESULT) {
		return "Incorrect result returned, expected " + to_string(SUM_RESULT);
	}
	return std::string();
}

virtual std::string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"" + GetQuery() + "\" without an index");
}
FINISH_BENCHMARK(DataSkipping)
