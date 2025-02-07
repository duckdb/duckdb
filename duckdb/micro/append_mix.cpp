#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

using namespace duckdb;

#define APPEND_MIX_BENCHMARK(PRIMARY_KEY)                                                                              \
	RandomEngine random;                                                                                               \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		if (!PRIMARY_KEY)                                                                                              \
			state->conn.Query("create table IF NOT EXISTS test(id INTEGER not null, area CHAR(6), age TINYINT not "    \
			                  "null, active TINYINT not null);");                                                      \
		else                                                                                                           \
			state->conn.Query("create table IF NOT EXISTS test(id INTEGER primary key, area CHAR(6), age TINYINT not " \
			                  "null, active TINYINT not null);");                                                      \
	}                                                                                                                  \
	int32_t get_random_age() {                                                                                         \
		/* 5, 10, 15 */                                                                                                \
		return 5 + 5 * random.NextRandom(0, 3);                                                                        \
	}                                                                                                                  \
	bool get_random_bool() {                                                                                           \
		return random.NextRandom() > 0.5;                                                                              \
	}                                                                                                                  \
	int32_t get_random_active() {                                                                                      \
		return int32_t(random.NextRandom(0, 2));                                                                       \
	}                                                                                                                  \
	void get_random_area_code(char *area_code) {                                                                       \
		uint32_t code = uint32_t(random.NextRandom(0, 999999));                                                        \
		auto endptr = area_code + 6;                                                                                   \
		NumericHelper::FormatUnsigned(code, endptr);                                                                   \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state) override {                                                          \
		state->conn.Query("BEGIN TRANSACTION");                                                                        \
		Appender appender(state->conn, "test");                                                                        \
		for (int32_t i = 0; i < 10000000; i++) {                                                                       \
			appender.BeginRow();                                                                                       \
			appender.Append<int32_t>(i);                                                                               \
			if (get_random_bool()) {                                                                                   \
				char area_code[6] = {'0', '0', '0', '0', '0', '0'};                                                    \
				get_random_area_code(area_code);                                                                       \
				appender.Append<string_t>(string_t(area_code, 6));                                                     \
			} else {                                                                                                   \
				appender.Append<std::nullptr_t>(nullptr);                                                              \
			}                                                                                                          \
			appender.Append<int32_t>(get_random_age());                                                                \
			appender.Append<int32_t>(get_random_active());                                                             \
			appender.EndRow();                                                                                         \
		}                                                                                                              \
		appender.Close();                                                                                              \
		state->conn.Query("COMMIT");                                                                                   \
	}                                                                                                                  \
	void Cleanup(DuckDBBenchmarkState *state) override {                                                               \
		state->conn.Query("DROP INDEX IF EXISTS pk_index");                                                            \
		state->conn.Query("DROP TABLE IF EXISTS test");                                                                \
		Load(state);                                                                                                   \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return "Append 10M rows to a table using an Appender";                                                         \
	}                                                                                                                  \
	optional_idx Timeout(const BenchmarkConfiguration &config) override {                                              \
		return 600;                                                                                                    \
	}

DUCKDB_BENCHMARK(Appender10MRows, "[append_mix]")
APPEND_MIX_BENCHMARK(false);
FINISH_BENCHMARK(Appender10MRows)

DUCKDB_BENCHMARK(Appender10MRowsPrimaryKey, "[append_mix]")
APPEND_MIX_BENCHMARK(true);
FINISH_BENCHMARK(Appender10MRowsPrimaryKey)

DUCKDB_BENCHMARK(Appender10MRowsDisk, "[append_mix]")
APPEND_MIX_BENCHMARK(false);
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Appender10MRowsDisk)

DUCKDB_BENCHMARK(Appender10MRowsDiskPrimaryKey, "[append_mix]")
APPEND_MIX_BENCHMARK(true);
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Appender10MRowsDiskPrimaryKey)
