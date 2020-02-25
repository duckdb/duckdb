#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define CAST_COUNT 10000000

DUCKDB_BENCHMARK(CastIntToString, "[cast]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE integers(i INTEGER);");
	Appender appender(state->conn, "integers");
	// insert the elements into the database
	for (int i = 0; i < CAST_COUNT; i++) {
		appender.AppendRow(i);
	}
}
string GetQuery() override {
	return "SELECT CAST(i AS VARCHAR) FROM integers";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Cast integer to string";
}
FINISH_BENCHMARK(CastIntToString)

DUCKDB_BENCHMARK(CastDoubleToString, "[cast]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE doubles(i DOUBLE);");
	Appender appender(state->conn, "doubles");
	// insert the elements into the database
	for (double i = 0; i < CAST_COUNT; i += 0.8) {
		appender.AppendRow(i);
	}
}
string GetQuery() override {
	return "SELECT CAST(i AS VARCHAR) FROM doubles";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Cast double to string";
}
FINISH_BENCHMARK(CastDoubleToString)

DUCKDB_BENCHMARK(CastStringToInt, "[cast]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE varchars(i VARCHAR);");
	Appender appender(state->conn, "varchars");
	// insert the elements into the database
	for (int i = 0; i < CAST_COUNT; i++) {
		appender.AppendRow(Value(to_string(i)));
	}
}
string GetQuery() override {
	return "SELECT CAST(i AS INTEGER) FROM varchars";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Cast int to string";
}
FINISH_BENCHMARK(CastStringToInt)

DUCKDB_BENCHMARK(CastStringToDouble, "[cast]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE varchars(i VARCHAR);");
	Appender appender(state->conn, "varchars");
	// insert the elements into the database
	for (int i = 0; i < CAST_COUNT; i++) {
		appender.AppendRow(Value(to_string(i)));
	}
}
string GetQuery() override {
	return "SELECT CAST(i AS DOUBLE) FROM varchars";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Cast double to string";
}
FINISH_BENCHMARK(CastStringToDouble)