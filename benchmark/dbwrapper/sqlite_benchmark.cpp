#include "sqlite_benchmark.hpp"

#include "sqlite_transfer.hpp"

using namespace duckdb;
using namespace std;

SQLiteBenchmark::SQLiteBenchmark(unique_ptr<DuckDBBenchmark> duckdb)
    : Benchmark(true, "sqlite_" + duckdb->name, "sqlite_" + duckdb->group), duckdb_benchmark(move(duckdb)) {
}

unique_ptr<BenchmarkState> SQLiteBenchmark::Initialize() {
	auto sqlite_state = make_unique<SQLiteBenchmarkState>();
	// first load the data into DuckDB
	auto duckdb_benchmark_state = duckdb_benchmark->Initialize();
	auto &duckdb_state = (DuckDBBenchmarkState &)*duckdb_benchmark_state;
	if (sqlite3_open(":memory:", &sqlite_state->db) != SQLITE_OK) {
		return nullptr;
	}
	// then transfer the data to SQLite
	sqlite::TransferDatabase(duckdb_state.conn, sqlite_state->db);
	// get the types of the query
	auto duckdb_result = duckdb_state.conn.Query(duckdb_benchmark->GetQuery());
	if (!duckdb_result->success) {
		return nullptr;
	}
	sqlite_state->types = duckdb_result->sql_types;
	return move(sqlite_state);
}

void SQLiteBenchmark::Run(BenchmarkState *state_) {
	auto state = (SQLiteBenchmarkState *)state_;
	auto query = duckdb_benchmark->GetQuery();
	state->result = sqlite::QueryDatabase(state->types, state->db, query, state->interrupt);
}

void SQLiteBenchmark::Cleanup(BenchmarkState *state_) {
}

string SQLiteBenchmark::Verify(BenchmarkState *state_) {
	auto state = (SQLiteBenchmarkState *)state_;
	if (!state->result) {
		return "No result!";
	}
	return duckdb_benchmark->VerifyResult(state->result.get());
}

string SQLiteBenchmark::GetLogOutput(BenchmarkState *state_) {
	return "";
}

void SQLiteBenchmark::Interrupt(BenchmarkState *state_) {
	auto state = (SQLiteBenchmarkState *)state_;
	state->interrupt = 1;
}

string SQLiteBenchmark::BenchmarkInfo() {
	return duckdb_benchmark->BenchmarkInfo();
}
