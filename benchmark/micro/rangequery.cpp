#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include "main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define RANGE_QUERY_ROW_COUNT 100000000
#define RANGE_QUERY_ENTRY_LOW 15000000
#define RANGE_QUERY_ENTRY_HIGH 16000000
#define SUM_RESULT 15499984500000

DUCKDB_BENCHMARK(RangeQueryWithoutIndex, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);");
	Appender appender(state->db, DEFAULT_SCHEMA, "integers");
	// insert the elements into the database
	for (size_t i = 0; i < RANGE_QUERY_ROW_COUNT; i++) {
		appender.BeginRow();
		appender.AppendInteger(i);
		appender.AppendInteger(i);
		appender.EndRow();
	}
	appender.Commit();
}
virtual std::string GetQuery() {
	return "SELECT sum(j) FROM integers WHERE i > " + to_string(RANGE_QUERY_ENTRY_LOW) + " and i < " +
	       to_string(RANGE_QUERY_ENTRY_HIGH);
}

virtual std::string VerifyResult(DuckDBResult *result) {
	if (!result->GetSuccess()) {
		return result->GetErrorMessage();
	}
	if (result->size() != 1) {
		return "Incorrect amount of rows in result";
	}
	if (result->column_count() != 1) {
		return "Incorrect amount of columns";
	}
	if (result->GetValue<int>(0, 0) != SUM_RESULT) {
		return "Incorrect result returned, expected " + to_string(result->GetValue<int>(0, 0));
	}
	return std::string();
}

virtual std::string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"" + GetQuery() + "\" without an index");
}
FINISH_BENCHMARK(RangeQueryWithoutIndex)

DUCKDB_BENCHMARK(RangeQueryWithIndex, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);");
	Appender appender(state->db, DEFAULT_SCHEMA, "integers");
	// insert the elements into the database
	for (size_t i = 0; i < RANGE_QUERY_ROW_COUNT; i++) {
		appender.BeginRow();
		appender.AppendInteger(i);
		appender.AppendInteger(i);
		appender.EndRow();
	}
	appender.Commit();
	state->conn.Query("CREATE INDEX i_index ON integers(i)");
}

virtual std::string GetQuery() {
	return "SELECT sum(j) FROM integers WHERE i > " + to_string(RANGE_QUERY_ENTRY_LOW) + " and i < " +
	       to_string(RANGE_QUERY_ENTRY_HIGH);
}

virtual std::string VerifyResult(DuckDBResult *result) {
	if (!result->GetSuccess()) {
		return result->GetErrorMessage();
	}
	if (result->size() != 1) {
		return "Incorrect amount of rows in result";
	}
	if (result->column_count() != 1) {
		return "Incorrect amount of columns";
	}
	if (result->GetValue<int>(0, 0) != SUM_RESULT) {
		return "Incorrect result returned, expected " + to_string(RANGE_QUERY_ENTRY_LOW + 2);
	}
	return std::string();
}

virtual std::string BenchmarkInfo() {
	return StringUtil::Format("Runs the following query: \"" + GetQuery() + "\" with an index");
}
FINISH_BENCHMARK(RangeQueryWithIndex)
