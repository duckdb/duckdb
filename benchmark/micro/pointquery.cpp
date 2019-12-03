#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define POINT_QUERY_ROW_COUNT 100000000
#define POINT_QUERY_ENTRY 50000

DUCKDB_BENCHMARK(PointQueryWithoutIndex, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);");
	Appender appender(state->conn, "integers"); // insert the elements into the database
	for (size_t i = 0; i < POINT_QUERY_ROW_COUNT; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.Append<int32_t>(i + 2);
		appender.EndRow();
	}
}

virtual string GetQuery() {
	return "SELECT j FROM integers WHERE i=" + to_string(POINT_QUERY_ENTRY);
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
	if (materialized.GetValue<int32_t>(0, 0) != POINT_QUERY_ENTRY + 2) {
		return "Incorrect result returned, expected " + to_string(POINT_QUERY_ENTRY + 2);
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"" + GetQuery() + "\" without an index");
}
FINISH_BENCHMARK(PointQueryWithoutIndex)

DUCKDB_BENCHMARK(PointQueryWithIndexART, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);");
	Appender appender(state->conn, "integers"); // insert the elements into the database
	for (size_t i = 0; i < POINT_QUERY_ROW_COUNT; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.Append<int32_t>(i + 2);
		appender.EndRow();
	}
	appender.Close();
	state->conn.Query("CREATE INDEX i_index ON integers using art(i)");
}

virtual string GetQuery() {
	return "SELECT j FROM integers WHERE i=" + to_string(POINT_QUERY_ENTRY);
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
	if (materialized.GetValue<int32_t>(0, 0) != POINT_QUERY_ENTRY + 2) {
		return "Incorrect result returned, expected " + to_string(POINT_QUERY_ENTRY + 2);
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"" + GetQuery() + "\" with an ART index");
}
FINISH_BENCHMARK(PointQueryWithIndexART)
