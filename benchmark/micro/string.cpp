#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define STRING_COUNT 1000000
#define STRING_LENGTH 4

#define STRING_DATA_GEN_BODY(STRING_LENGTH)                                                                            \
	static constexpr const char *chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";                                                 \
	static string GenerateString(std::uniform_int_distribution<> &distribution, std::mt19937 &gen) {                   \
		string result;                                                                                                 \
		for (size_t i = 0; i < STRING_LENGTH; i++) {                                                                   \
			result += string(1, chars[distribution(gen)]);                                                             \
		}                                                                                                              \
		return result;                                                                                                 \
	}                                                                                                                  \
	void Load(DuckDBBenchmarkState *state) override {                                                                   \
		std::uniform_int_distribution<> distribution(0, strlen(chars) - 1);                                            \
		std::mt19937 gen;                                                                                              \
		gen.seed(42);                                                                                                  \
		state->conn.Query("CREATE TABLE strings(s1 VARCHAR, s2 VARCHAR);");                                            \
		Appender appender(state->conn, "strings");                                                                     \
		for (size_t i = 0; i < STRING_COUNT; i++) {                                                                    \
			appender.BeginRow();                                                                                       \
			appender.Append<Value>(Value(GenerateString(distribution, gen)));                                          \
			appender.Append<Value>(Value(GenerateString(distribution, gen)));                                          \
			appender.EndRow();                                                                                         \
		}                                                                                                              \
		appender.Close();                                                                                              \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                 \
		if (!result->success) {                                                                                        \
			return result->error;                                                                                      \
		}                                                                                                              \
		return string();                                                                                               \
	}


DUCKDB_BENCHMARK(StringConcatShort, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT s1 || s2 FROM strings";
}
string BenchmarkInfo() override {
	return "STRING CONCAT";
}
FINISH_BENCHMARK(StringConcatShort)

DUCKDB_BENCHMARK(StringConcatLong, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
	return "SELECT s1 || s2 FROM strings";
}
string BenchmarkInfo() override {
	return "STRING CONCAT";
}
FINISH_BENCHMARK(StringConcatLong)

DUCKDB_BENCHMARK(StringEquality, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
	return "SELECT s1=s2 FROM strings";
}
string BenchmarkInfo() override {
	return "STRING COMPARISON";
}
FINISH_BENCHMARK(StringEquality)

DUCKDB_BENCHMARK(StringGreaterThan, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
	return "SELECT s1>s2 FROM strings";
}
string BenchmarkInfo() override {
	return "STRING COMPARISON";
}
FINISH_BENCHMARK(StringGreaterThan)

DUCKDB_BENCHMARK(StringLengthShort, "[string]")
STRING_DATA_GEN_BODY(5)
string GetQuery() override {
	return "SELECT LENGTH(s1)+LENGTH(s2) FROM strings";
}
string BenchmarkInfo() override {
	return "STRING LENGTH";
}
FINISH_BENCHMARK(StringLengthShort)

DUCKDB_BENCHMARK(StringLengthLong, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
	return "SELECT LENGTH(s1)+LENGTH(s2) FROM strings";
}
string BenchmarkInfo() override {
	return "STRING LENGTH";
}
FINISH_BENCHMARK(StringLengthLong)

DUCKDB_BENCHMARK(StringAgg, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT STRING_AGG(s1, ' ') FROM strings";
}
string BenchmarkInfo() override {
	return "STRING LENGTH";
}
FINISH_BENCHMARK(StringAgg)
