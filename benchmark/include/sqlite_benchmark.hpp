//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// sqlite_benchmark.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_benchmark.hpp"
#include "sqlite3.h"

namespace duckdb {

struct SQLiteBenchmarkState : public BenchmarkState {
	//! The result object of the query
	unique_ptr<QueryResult> result;
	//! The SQLite Database Object
	sqlite3 *db = nullptr;
	//! Flag used to interrupt query execution. Set to 1 to interrupt query.
	volatile int interrupt = 0;
	//! The result types
	vector<SQLType> types;

	SQLiteBenchmarkState() : db(nullptr), interrupt(0) {
	}
	virtual ~SQLiteBenchmarkState() {
		if (db) {
			sqlite3_close(db);
		}
	}
};

class SQLiteBenchmark : public Benchmark {
public:
	SQLiteBenchmark(unique_ptr<DuckDBBenchmark> duckdb);

	unique_ptr<BenchmarkState> Initialize() override;
	void Run(BenchmarkState *state_) override;
	void Cleanup(BenchmarkState *state_) override;
	string Verify(BenchmarkState *state_) override;
	string GetLogOutput(BenchmarkState *state_) override;
	void Interrupt(BenchmarkState *state_) override;
	string BenchmarkInfo() override;

private:
	unique_ptr<DuckDBBenchmark> duckdb_benchmark;
};

} // namespace duckdb
