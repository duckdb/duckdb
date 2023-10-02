#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

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
			state->conn.Query("INSERT INTO integers VALUES (" + std::to_string(i) + ")");                              \
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

DUCKDB_BENCHMARK(Append100KIntegersINSERT, "[append]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER)", false)
FINISH_BENCHMARK(Append100KIntegersINSERT)

DUCKDB_BENCHMARK(Append100KIntegersINSERTDisk, "[append]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER)", false)
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersINSERTDisk)

DUCKDB_BENCHMARK(Append100KIntegersINSERTPrimary, "[append]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER PRIMARY KEY)", false)
FINISH_BENCHMARK(Append100KIntegersINSERTPrimary)

DUCKDB_BENCHMARK(Append100KIntegersINSERTAutoCommit, "[append]")
APPEND_BENCHMARK_INSERT("CREATE TABLE integers(i INTEGER)", true)
FINISH_BENCHMARK(Append100KIntegersINSERTAutoCommit)

//////////////
// PREPARED //
//////////////
struct DuckDBPreparedState : public DuckDBBenchmarkState {
	duckdb::unique_ptr<PreparedStatement> prepared;

	DuckDBPreparedState(string path) : DuckDBBenchmarkState(path) {
	}
	virtual ~DuckDBPreparedState() {
	}
};

#define APPEND_BENCHMARK_PREPARED(CREATE_STATEMENT)                                                                    \
	duckdb::unique_ptr<DuckDBBenchmarkState> CreateBenchmarkState() override {                                         \
		auto result = make_uniq<DuckDBPreparedState>(GetDatabasePath());                                               \
		return std::move(result);                                                                                      \
	}                                                                                                                  \
	void Load(DuckDBBenchmarkState *state_p) override {                                                                \
		auto state = (DuckDBPreparedState *)state_p;                                                                   \
		state->conn.Query(CREATE_STATEMENT);                                                                           \
		state->prepared = state->conn.Prepare("INSERT INTO integers VALUES ($1)");                                     \
	}                                                                                                                  \
	void RunBenchmark(DuckDBBenchmarkState *state_p) override {                                                        \
		auto state = (DuckDBPreparedState *)state_p;                                                                   \
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

DUCKDB_BENCHMARK(Append100KIntegersPREPARED, "[append]")
APPEND_BENCHMARK_PREPARED("CREATE TABLE integers(i INTEGER)")
FINISH_BENCHMARK(Append100KIntegersPREPARED)

DUCKDB_BENCHMARK(Append100KIntegersPREPAREDDisk, "[append]")
APPEND_BENCHMARK_PREPARED("CREATE TABLE integers(i INTEGER)")
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersPREPAREDDisk)

DUCKDB_BENCHMARK(Append100KIntegersPREPAREDPrimary, "[append]")
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
		state->conn.Query("BEGIN TRANSACTION");                                                                        \
		Appender appender(state->conn, "integers");                                                                    \
		for (int32_t i = 0; i < 100000; i++) {                                                                         \
			appender.BeginRow();                                                                                       \
			appender.Append<int32_t>(i);                                                                               \
			appender.EndRow();                                                                                         \
		}                                                                                                              \
		appender.Close();                                                                                              \
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
		return "Append 100K 4-byte integers to a table using an Appender";                                             \
	}

DUCKDB_BENCHMARK(Append100KIntegersAPPENDER, "[append]")
APPEND_BENCHMARK_APPENDER("CREATE TABLE integers(i INTEGER)")
FINISH_BENCHMARK(Append100KIntegersAPPENDER)

DUCKDB_BENCHMARK(Append100KIntegersAPPENDERDisk, "[append]")
APPEND_BENCHMARK_APPENDER("CREATE TABLE integers(i INTEGER)")
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersAPPENDERDisk)

DUCKDB_BENCHMARK(Append100KIntegersAPPENDERPrimary, "[append]")
APPEND_BENCHMARK_APPENDER("CREATE TABLE integers(i INTEGER PRIMARY KEY)")
FINISH_BENCHMARK(Append100KIntegersAPPENDERPrimary)

///////////////
// COPY INTO //
///////////////
#define APPEND_BENCHMARK_COPY(CREATE_STATEMENT)                                                                        \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		state->conn.Query("CREATE TABLE integers(i INTEGER)");                                                         \
		Appender appender(state->conn, "integers");                                                                    \
		for (int32_t i = 0; i < 100000; i++) {                                                                         \
			appender.BeginRow();                                                                                       \
			appender.Append<int32_t>(i);                                                                               \
			appender.EndRow();                                                                                         \
		}                                                                                                              \
		appender.Close();                                                                                              \
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

DUCKDB_BENCHMARK(Append100KIntegersCOPY, "[append]")
APPEND_BENCHMARK_COPY("CREATE TABLE integers(i INTEGER)")
FINISH_BENCHMARK(Append100KIntegersCOPY)

DUCKDB_BENCHMARK(Append100KIntegersCOPYDisk, "[append]")
APPEND_BENCHMARK_COPY("CREATE TABLE integers(i INTEGER)")
bool InMemory() override {
	return false;
}
FINISH_BENCHMARK(Append100KIntegersCOPYDisk)

DUCKDB_BENCHMARK(Append100KIntegersCOPYPrimary, "[append]")
APPEND_BENCHMARK_COPY("CREATE TABLE integers(i INTEGER PRIMARY KEY)")
FINISH_BENCHMARK(Append100KIntegersCOPYPrimary)

DUCKDB_BENCHMARK(Write100KIntegers, "[append]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE integers(i INTEGER)");
	Appender appender(state->conn, "integers");
	for (int32_t i = 0; i < 100000; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.EndRow();
	}
}
string GetQuery() override {
	return "COPY integers TO 'integers.csv' DELIMITER '|' HEADER";
}
string VerifyResult(QueryResult *result) override {
	if (result->HasError()) {
		return result->GetError();
	}
	return string();
}
string BenchmarkInfo() override {
	return "Write 100K 4-byte integers to CSV";
}
FINISH_BENCHMARK(Write100KIntegers)
