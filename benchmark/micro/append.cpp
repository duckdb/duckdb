#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "main/appender.hpp"

using namespace duckdb;
using namespace std;

//////////////
// INSERT //
//////////////
#define APPEND_BENCHMARK_INSERT(CREATE_STATEMENT, AUTO_COMMIT)                                                         \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		state->conn.Query(CREATE_STATEMENT);                                                                           \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state) override {                                                          \
		if (!AUTO_COMMIT)                                                                                              \
			state->conn.Query("BEGIN TRANSACTION");                                                                    \
		for (int32_t i = 0; i < 100000; i++) {                                                                         \
			state->conn.Query("INSERT INTO integers VALUES (" + to_string(i) + ")");                                   \
		}                                                                                                              \
		if (!AUTO_COMMIT)                                                                                              \
			state->conn.Query("COMMIT");                                                                               \
	}                                                                                                                  \
	void Cleanup(DuckDBBenchmarkState *state) override {                                                               \
		state->conn.Query("DROP TABLE integers");                                                                      \
		Load(state);                                                                                                   \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return "Append 100K 4-byte integers to a table using a series of INSERT INTO statements";                      \
	}

DUCKDB_BENCHMARK(Append100KIntegersINSERT, "[csv]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER)", false)
FINISH_BENCHMARK(Append100KIntegersINSERT)

DUCKDB_BENCHMARK(Append100KIntegersINSERTDisk, "[csv]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER)", false)
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersINSERTDisk)

DUCKDB_BENCHMARK(Append100KIntegersINSERTPrimary, "[csv]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER PRIMARY KEY)", false)
FINISH_BENCHMARK(Append100KIntegersINSERTPrimary)

DUCKDB_BENCHMARK(Append100KIntegersINSERTAutoCommit, "[csv]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER)", true)
FINISH_BENCHMARK(Append100KIntegersINSERTAutoCommit)

DUCKDB_BENCHMARK(Append100KIntegersINSERTAutoCommitDisk, "[csv]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER)", true)
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersINSERTAutoCommitDisk)

//////////////
// PREPARED //
//////////////
struct DuckDBPreparedState : public DuckDBBenchmarkState {
	unique_ptr<PreparedStatement> prepared;

	DuckDBPreparedState(string path) : DuckDBBenchmarkState(path) {
	}
	virtual ~DuckDBPreparedState() {
	}
};

#define APPEND_BENCHMARK_PREPARED(CREATE_STATEMENT)                                                                    \
	unique_ptr<DuckDBBenchmarkState> CreateBenchmarkState() override {                                                 \
		auto result = make_unique<DuckDBPreparedState>(GetDatabasePath());                                             \
		return move(result);                                                                                           \
	}                                                                                                                  \
	void Load(DuckDBBenchmarkState *state_) override {                                                                 \
		auto state = (DuckDBPreparedState *)state_;                                                                    \
		state->conn.Query(CREATE_STATEMENT);                                                                           \
		state->prepared = state->conn.Prepare("INSERT INTO integers VALUES ($1)");                                     \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state_) override {                                                         \
		auto state = (DuckDBPreparedState *)state_;                                                                    \
		state->conn.Query("BEGIN TRANSACTION");                                                                        \
		for (int32_t i = 0; i < 100000; i++) {                                                                         \
			state->prepared->Execute(i);                                                                               \
		}                                                                                                              \
		state->conn.Query("COMMIT");                                                                                   \
	}                                                                                                                  \
	void Cleanup(DuckDBBenchmarkState *state) override {                                                               \
		state->conn.Query("DROP TABLE integers");                                                                      \
		Load(state);                                                                                                   \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return "Append 100K 4-byte integers to a table using a series of prepared INSERT INTO statements";             \
	}

DUCKDB_BENCHMARK(Append100KIntegersPREPARED, "[csv]")
APPEND_BENCHMARK_PREPARED("CREATE TABLE integers(i INTEGER)")
FINISH_BENCHMARK(Append100KIntegersPREPARED)

DUCKDB_BENCHMARK(Append100KIntegersPREPAREDDisk, "[csv]")
APPEND_BENCHMARK_PREPARED("CREATE TABLE integers(i INTEGER)")
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersPREPAREDDisk)

DUCKDB_BENCHMARK(Append100KIntegersPREPAREDPrimary, "[csv]")
APPEND_BENCHMARK_PREPARED("CREATE TABLE integers(i INTEGER PRIMARY KEY)")
FINISH_BENCHMARK(Append100KIntegersPREPAREDPrimary)

//////////////
// APPENDER //
//////////////
#define APPEND_BENCHMARK_APPENDER(CREATE_STATEMENT)                                                                    \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		state->conn.Query(CREATE_STATEMENT);                                                                           \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state) override {                                                          \
		Appender appender(state->db, DEFAULT_SCHEMA, "integers");                                                      \
		for (int32_t i = 0; i < 100000; i++) {                                                                         \
			appender.BeginRow();                                                                                       \
			appender.AppendInteger(i);                                                                                 \
			appender.EndRow();                                                                                         \
		}                                                                                                              \
		appender.Commit();                                                                                             \
	}                                                                                                                  \
	void Cleanup(DuckDBBenchmarkState *state) override {                                                               \
		state->conn.Query("DROP TABLE integers");                                                                      \
		Load(state);                                                                                                   \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return "Append 100K 4-byte integers to a table using an Appender";                                             \
	}

DUCKDB_BENCHMARK(Append100KIntegersAPPENDER, "[csv]")
APPEND_BENCHMARK_APPENDER("CREATE TABLE integers(i INTEGER)")
FINISH_BENCHMARK(Append100KIntegersAPPENDER)

DUCKDB_BENCHMARK(Append100KIntegersAPPENDERDisk, "[csv]")
APPEND_BENCHMARK_APPENDER("CREATE TABLE integers(i INTEGER)")
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersAPPENDERDisk)

DUCKDB_BENCHMARK(Append100KIntegersAPPENDERPrimary, "[csv]")
APPEND_BENCHMARK_APPENDER("CREATE TABLE integers(i INTEGER PRIMARY KEY)")
FINISH_BENCHMARK(Append100KIntegersAPPENDERPrimary)

///////////////
// COPY INTO //
///////////////
#define APPEND_BENCHMARK_COPY(CREATE_STATEMENT)                                                                        \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		state->conn.Query("CREATE TABLE integers(i INTEGER)");                                                         \
		Appender appender(state->db, DEFAULT_SCHEMA, "integers");                                                      \
		for (int32_t i = 0; i < 100000; i++) {                                                                         \
			appender.BeginRow();                                                                                       \
			appender.AppendInteger(i);                                                                                 \
			appender.EndRow();                                                                                         \
		}                                                                                                              \
		appender.Commit();                                                                                             \
		state->conn.Query("COPY integers TO 'integers.csv' DELIMITER '|'");                                            \
		state->conn.Query("DROP TABLE integers");                                                                      \
		state->conn.Query(CREATE_STATEMENT);                                                                           \
	}                                                                                                                  \
	string GetQuery() override {                                                                                       \
		return "COPY integers FROM 'integers.csv' DELIMITER '|'";                                                      \
	}                                                                                                                  \
	void Cleanup(DuckDBBenchmarkState *state) override {                                                               \
		state->conn.Query("DROP TABLE integers");                                                                      \
		state->conn.Query(CREATE_STATEMENT);                                                                           \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return "Append 100K 4-byte integers to a table using the COPY INTO statement";                                 \
	}

DUCKDB_BENCHMARK(Append100KIntegersCOPY, "[csv]")
APPEND_BENCHMARK_COPY("CREATE TABLE integers(i INTEGER)")
FINISH_BENCHMARK(Append100KIntegersCOPY)

DUCKDB_BENCHMARK(Append100KIntegersCOPYDisk, "[csv]")
APPEND_BENCHMARK_COPY("CREATE TABLE integers(i INTEGER)")
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersCOPYDisk)

DUCKDB_BENCHMARK(Append100KIntegersCOPYPrimary, "[csv]")
APPEND_BENCHMARK_COPY("CREATE TABLE integers(i INTEGER PRIMARY KEY)")
FINISH_BENCHMARK(Append100KIntegersCOPYPrimary)
