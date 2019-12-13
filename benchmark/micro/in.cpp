#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

#define IN_LIST_ROW_COUNT 1000000
#define STRING_LENGTH 4

#define IN_QUERY_BODY(INCOUNT)                                                                                         \
	static constexpr const char *chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";                                                 \
	string in_list;                                                                                                    \
	static string GenerateString(std::uniform_int_distribution<> &distribution, std::mt19937 &gen) {                   \
		string result;                                                                                                 \
		for (size_t i = 0; i < STRING_LENGTH; i++) {                                                                   \
			result += string(chars[distribution(gen)], 1);                                                             \
		}                                                                                                              \
		return result;                                                                                                 \
	}                                                                                                                  \
	virtual void Load(DuckDBBenchmarkState *state) {                                                                   \
		std::uniform_int_distribution<> distribution(0, strlen(chars));                                                \
		std::mt19937 gen;                                                                                              \
		gen.seed(42);                                                                                                  \
		state->conn.Query("CREATE TABLE strings(s VARCHAR);");                                                         \
		Appender appender(state->conn, "strings");                                                                     \
		for (size_t i = 0; i < IN_LIST_ROW_COUNT; i++) {                                                               \
			appender.BeginRow();                                                                                       \
			appender.Append<Value>(Value(GenerateString(distribution, gen)));                                          \
			appender.EndRow();                                                                                         \
		}                                                                                                              \
		appender.Close();                                                                                              \
		for (size_t i = 0; i < INCOUNT; i++) {                                                                         \
			in_list += "'" + GenerateString(distribution, gen) + "'";                                                  \
			if (i != INCOUNT - 1) {                                                                                    \
				in_list += ", ";                                                                                       \
			}                                                                                                          \
		}                                                                                                              \
	}                                                                                                                  \
	virtual string GetQuery() {                                                                                        \
		return "SELECT * FROM strings WHERE s IN (" + in_list + ")";                                                   \
	}                                                                                                                  \
	virtual string VerifyResult(QueryResult *result) {                                                                 \
		if (!result->success) {                                                                                        \
			return result->error;                                                                                      \
		}                                                                                                              \
		return string();                                                                                               \
	}                                                                                                                  \
	virtual string BenchmarkInfo() {                                                                                   \
		return StringUtil::Format("Runs the following query: \"" + GetQuery() + "\"");                                 \
	}

DUCKDB_BENCHMARK(InList0001Entry, "[in]")
IN_QUERY_BODY(1)
FINISH_BENCHMARK(InList0001Entry)

DUCKDB_BENCHMARK(InList0002Entry, "[in]")
IN_QUERY_BODY(2)
FINISH_BENCHMARK(InList0002Entry)

DUCKDB_BENCHMARK(InList0004Entry, "[in]")
IN_QUERY_BODY(4)
FINISH_BENCHMARK(InList0004Entry)

DUCKDB_BENCHMARK(InList0008Entry, "[in]")
IN_QUERY_BODY(8)
FINISH_BENCHMARK(InList0008Entry)

DUCKDB_BENCHMARK(InList0016Entry, "[in]")
IN_QUERY_BODY(16)
FINISH_BENCHMARK(InList0016Entry)

DUCKDB_BENCHMARK(InList0032Entry, "[in]")
IN_QUERY_BODY(32)
FINISH_BENCHMARK(InList0032Entry)

DUCKDB_BENCHMARK(InList0064Entry, "[in]")
IN_QUERY_BODY(64)
FINISH_BENCHMARK(InList0064Entry)

DUCKDB_BENCHMARK(InList0128Entry, "[in]")
IN_QUERY_BODY(128)
FINISH_BENCHMARK(InList0128Entry)

DUCKDB_BENCHMARK(InList0256Entry, "[in]")
IN_QUERY_BODY(256)
FINISH_BENCHMARK(InList0256Entry)

DUCKDB_BENCHMARK(InList0512Entry, "[in]")
IN_QUERY_BODY(512)
FINISH_BENCHMARK(InList0512Entry)

DUCKDB_BENCHMARK(InList1024Entry, "[in]")
IN_QUERY_BODY(1024)
FINISH_BENCHMARK(InList1024Entry)

DUCKDB_BENCHMARK(InList2048Entry, "[in]")
IN_QUERY_BODY(2048)
FINISH_BENCHMARK(InList2048Entry)
