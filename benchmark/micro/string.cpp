#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define STRING_COUNT 10000000
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
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
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
	string VerifyResult(QueryResult *result) override {                                                                \
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

DUCKDB_BENCHMARK(StringEqualityShort, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT s1=s2 FROM strings";
}
string BenchmarkInfo() override {
	return "STRING COMPARISON";
}
FINISH_BENCHMARK(StringEqualityShort)

DUCKDB_BENCHMARK(StringEqualityLong, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
	return "SELECT s1=s2 FROM strings";
}
string BenchmarkInfo() override {
	return "STRING COMPARISON";
}
FINISH_BENCHMARK(StringEqualityLong)

DUCKDB_BENCHMARK(StringGreaterThanShort, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT s1>s2 FROM strings";
}
string BenchmarkInfo() override {
	return "STRING COMPARISON";
}
FINISH_BENCHMARK(StringGreaterThanShort)

DUCKDB_BENCHMARK(StringGreaterThanLong, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
	return "SELECT s1>s2 FROM strings";
}
string BenchmarkInfo() override {
	return "STRING COMPARISON";
}
FINISH_BENCHMARK(StringGreaterThanLong)

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

DUCKDB_BENCHMARK(StringAggShort, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT STRING_AGG(s1, ' ') FROM strings";
}
string BenchmarkInfo() override {
	return "STRING LENGTH";
}
FINISH_BENCHMARK(StringAggShort)

DUCKDB_BENCHMARK(StringAggLong, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
	return "SELECT STRING_AGG(s1, ' ') FROM strings";
}
string BenchmarkInfo() override {
	return "STRING LENGTH";
}
FINISH_BENCHMARK(StringAggLong)

DUCKDB_BENCHMARK(StringInstr, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT INSTR(s1, 'h') FROM strings";
}
string BenchmarkInfo() override {
	return "STRING INSTR";
}
FINISH_BENCHMARK(StringInstr)

DUCKDB_BENCHMARK(StringInstrNull, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT INSTR(s1, '') FROM strings";
}
string BenchmarkInfo() override {
	return "STRING INSTR";
}
FINISH_BENCHMARK(StringInstrNull)

DUCKDB_BENCHMARK(StringRegex, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT REGEXP_MATCHES(s1, 'h') FROM strings";
}
string BenchmarkInfo() override {
	return "STRING REGEX";
}
FINISH_BENCHMARK(StringRegex)

DUCKDB_BENCHMARK(StringRegexNull, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT REGEXP_MATCHES(s1, '') FROM strings";
}
string BenchmarkInfo() override {
	return "STRING REGEX";
}
FINISH_BENCHMARK(StringRegexNull)

//----------------------- PREFIX1 benchmark ----------------------------------//
DUCKDB_BENCHMARK(StringPrefix, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
    return "SELECT prefix(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX early out";
}
FINISH_BENCHMARK(StringPrefix)

DUCKDB_BENCHMARK(StringPrefixInlined, "[string]")
STRING_DATA_GEN_BODY(12)
string GetQuery() override {
    return "SELECT prefix(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX inlined";
}
FINISH_BENCHMARK(StringPrefixInlined)

DUCKDB_BENCHMARK(StringPrefixPointer, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
    return "SELECT prefix(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX store pointer";
}
FINISH_BENCHMARK(StringPrefixPointer)

//----------------------- PREFIX2 benchmark ----------------------------------//
DUCKDB_BENCHMARK(StringPrefix222, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
    return "SELECT prefix2(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX222 early out";
}
FINISH_BENCHMARK(StringPrefix222)

DUCKDB_BENCHMARK(StringPrefixInlined222, "[string]")
STRING_DATA_GEN_BODY(12)
string GetQuery() override {
    return "SELECT prefix2(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX222 inlined";
}
FINISH_BENCHMARK(StringPrefixInlined222)

DUCKDB_BENCHMARK(StringPrefixPointer222, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
    return "SELECT prefix2(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX222 store pointer";
}
FINISH_BENCHMARK(StringPrefixPointer222)
//-----------------------------------------------------------------------------

//----------------------- PREFIX3 benchmark ----------------------------------//
DUCKDB_BENCHMARK(StringPrefix333, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
    return "SELECT prefix3(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX333 early out";
}
FINISH_BENCHMARK(StringPrefix333)

DUCKDB_BENCHMARK(StringPrefixInlined333, "[string]")
STRING_DATA_GEN_BODY(12)
string GetQuery() override {
    return "SELECT prefix3(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX333 inlined";
}
FINISH_BENCHMARK(StringPrefixInlined333)

DUCKDB_BENCHMARK(StringPrefixPointer333, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
    return "SELECT prefix3(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX333 store pointer";
}
FINISH_BENCHMARK(StringPrefixPointer333)
//-----------------------------------------------------------------------------

//----------------------- PREFIX4 benchmark ----------------------------------//
DUCKDB_BENCHMARK(StringPrefix444, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
    return "SELECT prefix4(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX444 early out";
}
FINISH_BENCHMARK(StringPrefix444)

DUCKDB_BENCHMARK(StringPrefixInlined444, "[string]")
STRING_DATA_GEN_BODY(12)
string GetQuery() override {
    return "SELECT prefix4(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX444 inlined";
}
FINISH_BENCHMARK(StringPrefixInlined444)

DUCKDB_BENCHMARK(StringPrefixPointer444, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
    return "SELECT prefix4(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX444 store pointer";
}
FINISH_BENCHMARK(StringPrefixPointer444)
//-----------------------------------------------------------------------------

//----------------------- PREFIX5 benchmark ----------------------------------//
DUCKDB_BENCHMARK(StringPrefix555, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
    return "SELECT prefix5(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX555 early out";
}
FINISH_BENCHMARK(StringPrefix555)

DUCKDB_BENCHMARK(StringPrefixInlined555, "[string]")
STRING_DATA_GEN_BODY(12)
string GetQuery() override {
    return "SELECT prefix5(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX555 inlined";
}
FINISH_BENCHMARK(StringPrefixInlined555)

DUCKDB_BENCHMARK(StringPrefixPointer555, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
    return "SELECT prefix5(s1, 'a') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX555 store pointer";
}
FINISH_BENCHMARK(StringPrefixPointer555)
//-----------------------------------------------------------------------------

//----------------------- PREFIX REGEX benchmark ----------------------------------//
DUCKDB_BENCHMARK(StringPrefixRegEX, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
    return "SELECT REGEXP_MATCHES(s1, 'a%') FROM strings";
}
string BenchmarkInfo() override {
    return "STRING PREFIX REGEX";
}
FINISH_BENCHMARK(StringPrefixRegEX)

//----------------------- PREFIX LIKE benchmark ----------------------------------//
DUCKDB_BENCHMARK(StringPrefixLike, "[string]")
STRING_DATA_GEN_BODY(4)
string GetQuery() override {
    return "SELECT s1 FROM strings WHERE s1 LIKE 'a%'";
}
string BenchmarkInfo() override {
    return "STRING PREFIX LIKE";
}
FINISH_BENCHMARK(StringPrefixLike)

DUCKDB_BENCHMARK(StringPrefixInlinedLike, "[string]")
STRING_DATA_GEN_BODY(12)
string GetQuery() override {
    return "SELECT s1 FROM strings WHERE s1 LIKE 'a%'";
}
string BenchmarkInfo() override {
    return "STRING PREFIX inlined LIKE";
}
FINISH_BENCHMARK(StringPrefixInlinedLike)

DUCKDB_BENCHMARK(StringPrefixPointerLike, "[string]")
STRING_DATA_GEN_BODY(20)
string GetQuery() override {
    return "SELECT s1 FROM strings WHERE s1 LIKE 'a%'";
}
string BenchmarkInfo() override {
    return "STRING PREFIX store pointer LIKE";
}
FINISH_BENCHMARK(StringPrefixPointerLike)
