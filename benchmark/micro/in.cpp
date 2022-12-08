#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;

#define IN_LIST_ROW_COUNT 1000000
#define STRING_LENGTH     4

#define IN_QUERY_BODY(INCOUNT, NOT_IN)                                                                                 \
	static constexpr const char *chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";                                                 \
	string in_list;                                                                                                    \
	static string GenerateString(std::uniform_int_distribution<> &distribution, std::mt19937 &gen) {                   \
		string result;                                                                                                 \
		for (size_t i = 0; i < STRING_LENGTH; i++) {                                                                   \
			result += string(1, chars[distribution(gen)]);                                                             \
		}                                                                                                              \
		return result;                                                                                                 \
	}                                                                                                                  \
	virtual void Load(DuckDBBenchmarkState *state) {                                                                   \
		std::uniform_int_distribution<> distribution(0, strlen(chars) - 1);                                            \
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
		return "SELECT * FROM strings WHERE s " + (NOT_IN ? string("NOT ") : string("")) + "IN (" + in_list + ")";     \
	}                                                                                                                  \
	virtual string VerifyResult(QueryResult *result) {                                                                 \
		if (result->HasError()) {                                                                                      \
			return result->GetError();                                                                                 \
		}                                                                                                              \
		return string();                                                                                               \
	}                                                                                                                  \
	virtual string BenchmarkInfo() {                                                                                   \
		return StringUtil::Format("Runs the following query: \"" + GetQuery() + "\"");                                 \
	}

DUCKDB_BENCHMARK(InList0001Entry, "[in]")
IN_QUERY_BODY(1, false)
FINISH_BENCHMARK(InList0001Entry)

DUCKDB_BENCHMARK(InList0002Entry, "[in]")
IN_QUERY_BODY(2, false)
FINISH_BENCHMARK(InList0002Entry)

DUCKDB_BENCHMARK(InList0004Entry, "[in]")
IN_QUERY_BODY(4, false)
FINISH_BENCHMARK(InList0004Entry)

DUCKDB_BENCHMARK(InList0008Entry, "[in]")
IN_QUERY_BODY(8, false)
FINISH_BENCHMARK(InList0008Entry)

DUCKDB_BENCHMARK(InList0016Entry, "[in]")
IN_QUERY_BODY(16, false)
FINISH_BENCHMARK(InList0016Entry)

DUCKDB_BENCHMARK(InList0032Entry, "[in]")
IN_QUERY_BODY(32, false)
FINISH_BENCHMARK(InList0032Entry)

DUCKDB_BENCHMARK(InList0064Entry, "[in]")
IN_QUERY_BODY(64, false)
FINISH_BENCHMARK(InList0064Entry)

DUCKDB_BENCHMARK(InList0128Entry, "[in]")
IN_QUERY_BODY(128, false)
FINISH_BENCHMARK(InList0128Entry)

DUCKDB_BENCHMARK(InList0256Entry, "[in]")
IN_QUERY_BODY(256, false)
FINISH_BENCHMARK(InList0256Entry)

DUCKDB_BENCHMARK(InList0512Entry, "[in]")
IN_QUERY_BODY(512, false)
FINISH_BENCHMARK(InList0512Entry)

DUCKDB_BENCHMARK(InList1024Entry, "[in]")
IN_QUERY_BODY(1024, false)
FINISH_BENCHMARK(InList1024Entry)

DUCKDB_BENCHMARK(InList2048Entry, "[in]")
IN_QUERY_BODY(2048, false)
FINISH_BENCHMARK(InList2048Entry)

DUCKDB_BENCHMARK(NotInList0001Entry, "[in]")
IN_QUERY_BODY(1, true)
FINISH_BENCHMARK(NotInList0001Entry)

DUCKDB_BENCHMARK(NotInList0002Entry, "[in]")
IN_QUERY_BODY(2, true)
FINISH_BENCHMARK(NotInList0002Entry)

DUCKDB_BENCHMARK(NotInList0004Entry, "[in]")
IN_QUERY_BODY(4, true)
FINISH_BENCHMARK(NotInList0004Entry)

DUCKDB_BENCHMARK(NotInList0008Entry, "[in]")
IN_QUERY_BODY(8, true)
FINISH_BENCHMARK(NotInList0008Entry)

DUCKDB_BENCHMARK(NotInList0016Entry, "[in]")
IN_QUERY_BODY(16, true)
FINISH_BENCHMARK(NotInList0016Entry)

DUCKDB_BENCHMARK(NotInList0032Entry, "[in]")
IN_QUERY_BODY(32, true)
FINISH_BENCHMARK(NotInList0032Entry)

DUCKDB_BENCHMARK(NotInList0064Entry, "[in]")
IN_QUERY_BODY(64, true)
FINISH_BENCHMARK(NotInList0064Entry)

DUCKDB_BENCHMARK(NotInList0128Entry, "[in]")
IN_QUERY_BODY(128, true)
FINISH_BENCHMARK(NotInList0128Entry)

DUCKDB_BENCHMARK(NotInList0256Entry, "[in]")
IN_QUERY_BODY(256, true)
FINISH_BENCHMARK(NotInList0256Entry)

DUCKDB_BENCHMARK(NotInList0512Entry, "[in]")
IN_QUERY_BODY(512, true)
FINISH_BENCHMARK(NotInList0512Entry)

DUCKDB_BENCHMARK(NotInList1024Entry, "[in]")
IN_QUERY_BODY(1024, true)
FINISH_BENCHMARK(NotInList1024Entry)

DUCKDB_BENCHMARK(NotInList2048Entry, "[in]")
IN_QUERY_BODY(2048, true)
FINISH_BENCHMARK(NotInList2048Entry)
