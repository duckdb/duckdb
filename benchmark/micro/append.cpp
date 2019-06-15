#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "main/appender.hpp"

using namespace duckdb;
using namespace std;

//////////////
// INSERT //
//////////////
DUCKDB_BENCHMARK(Append100KIntegersINSERT, "[csv]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE integers(i INTEGER)");
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("BEGIN TRANSACTION");
	for(int32_t i = 0; i < 100000; i++) {
		state->conn.Query("INSERT INTO integers VALUES (" + to_string(i) + ")");
	}
	state->conn.Query("COMMIT");
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DROP TABLE integers");
	Load(state);
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Append 100K 4-byte integers to a table using a series of INSERT INTO statements";
}
FINISH_BENCHMARK(Append100KIntegersINSERT)


//////////////
// PREPARED //
//////////////
struct DuckDBPreparedState : public DuckDBBenchmarkState {
	unique_ptr<PreparedStatement> prepared;

	virtual ~DuckDBPreparedState() {
	}
};

DUCKDB_BENCHMARK(Append100KIntegersPREPARED, "[csv]")
unique_ptr<DuckDBBenchmarkState> CreateBenchmarkState() override {
	auto result = make_unique<DuckDBPreparedState>();
	return move(result);
}
void Load(DuckDBBenchmarkState *state_) override {
	auto state = (DuckDBPreparedState*) state_;
	state->conn.Query("CREATE TABLE integers(i INTEGER)");
	state->prepared = state->conn.Prepare("INSERT INTO integers VALUES ($1)");

}
void RunBenchmark(DuckDBBenchmarkState *state_) override {
	auto state = (DuckDBPreparedState*) state_;
	state->conn.Query("BEGIN TRANSACTION");
	for(int32_t i = 0; i < 100000; i++) {
		state->prepared->Execute(i);
	}
	state->conn.Query("COMMIT");
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DROP TABLE integers");
	Load(state);
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Append 100K 4-byte integers to a table using a series of prepared INSERT INTO statements";
}
FINISH_BENCHMARK(Append100KIntegersPREPARED)

//////////////
// APPENDER //
//////////////
DUCKDB_BENCHMARK(Append100KIntegersAPPENDER, "[csv]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE integers(i INTEGER)");
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	Appender appender(state->db, DEFAULT_SCHEMA, "integers");
	for(int32_t i = 0; i < 100000; i++) {
		appender.BeginRow();
		appender.AppendInteger(i);
		appender.EndRow();
	}
	appender.Commit();
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DROP TABLE integers");
	Load(state);
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Append 100K 4-byte integers to a table using an Appender";
}
FINISH_BENCHMARK(Append100KIntegersAPPENDER)


///////////////
// COPY INTO //
///////////////
DUCKDB_BENCHMARK(Append100KIntegersCOPY, "[csv]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	state->conn.Query("CREATE TABLE integers(i INTEGER)");
	Appender appender(state->db, DEFAULT_SCHEMA, "integers");
	for(int32_t i = 0; i < 100000; i++) {
		appender.BeginRow();
		appender.AppendInteger(i);
		appender.EndRow();
	}
	appender.Commit();
	state->conn.Query("COPY integers TO 'integers.csv' DELIMITER '|'");
	state->conn.Query("DROP TABLE integers");
	state->conn.Query("CREATE TABLE integers(i INTEGER)");
}
string GetQuery() override {
	return "COPY integers FROM 'integers.csv' DELIMITER '|'";
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DROP TABLE integers");
	state->conn.Query("CREATE TABLE integers(i INTEGER)");
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Append 100K 4-byte integers to a table using the COPY INTO statement";
}
FINISH_BENCHMARK(Append100KIntegersCOPY)

