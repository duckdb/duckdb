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

DUCKDB_BENCHMARK(FormatIntToString, "[cast]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE integers(i INTEGER);");
	Appender appender(state->conn, "integers");
	// insert the elements into the database
	for (int i = 0; i < CAST_COUNT; i++) {
		appender.AppendRow(i);
	}
}
string GetQuery() override {
	return "SELECT format('{}', i) FROM integers";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Convert integer to string using format function";
}
FINISH_BENCHMARK(FormatIntToString)

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

DUCKDB_BENCHMARK(CastDateToString, "[cast]")
void Load(DuckDBBenchmarkState *state) override {
	std::uniform_int_distribution<> year_dist(1990, 2010), day_dist(1, 28), month_dist(1, 12);
	std::mt19937 gen;
	gen.seed(42);

	state->conn.Query("CREATE TABLE dates(d DATE);");
	Appender appender(state->conn, "dates");
	// insert the elements into the database
	for (int i = 0; i < CAST_COUNT; i++) {
		appender.AppendRow(Value::DATE(year_dist(gen), month_dist(gen), day_dist(gen)));
	}
}
string GetQuery() override {
	return "SELECT CAST(d AS VARCHAR) FROM dates";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Cast date to string";
}
FINISH_BENCHMARK(CastDateToString)

DUCKDB_BENCHMARK(CastTimeToString, "[cast]")
void Load(DuckDBBenchmarkState *state) override {
	std::uniform_int_distribution<> hour_dist(0, 23), min_dist(0, 59);
	std::mt19937 gen;
	gen.seed(42);

	state->conn.Query("CREATE TABLE times(d TIME);");
	Appender appender(state->conn, "times");
	// insert the elements into the database
	for (int i = 0; i < CAST_COUNT; i++) {
		appender.AppendRow(Value::TIME(hour_dist(gen), min_dist(gen), min_dist(gen), 0));
	}
}
string GetQuery() override {
	return "SELECT CAST(d AS VARCHAR) FROM times";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Cast time to string";
}
FINISH_BENCHMARK(CastTimeToString)

DUCKDB_BENCHMARK(CastTimestampToString, "[cast]")
void Load(DuckDBBenchmarkState *state) override {
	std::uniform_int_distribution<> year_dist(1990, 2010), day_dist(1, 28), month_dist(1, 12);
	std::uniform_int_distribution<> hour_dist(0, 23), min_dist(0, 59);
	std::mt19937 gen;
	gen.seed(42);

	state->conn.Query("CREATE TABLE timestamps(d TIMESTAMP);");
	Appender appender(state->conn, "timestamps");
	// insert the elements into the database
	for (int i = 0; i < CAST_COUNT; i++) {
		appender.AppendRow(Value::TIMESTAMP(year_dist(gen), month_dist(gen), day_dist(gen), hour_dist(gen),
		                                    min_dist(gen), min_dist(gen), 0));
	}
}
string GetQuery() override {
	return "SELECT CAST(d AS VARCHAR) FROM timestamps";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Cast timestamp to string";
}
FINISH_BENCHMARK(CastTimestampToString)
