//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb_benchmark.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "benchmark.hpp"
#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "test_helpers.hpp"

namespace duckdb {

//! Base class for any state that has to be kept by a Benchmark
struct DuckDBBenchmarkState : public BenchmarkState {
	DuckDB db;
	Connection conn;
	unique_ptr<QueryResult> result;

	DuckDBBenchmarkState(string path) : db(path.empty() ? nullptr : path.c_str()), conn(db) {
		conn.EnableProfiling();
	}
	virtual ~DuckDBBenchmarkState() {
	}
};

//! The base Benchmark class is a base class that is used to create and register
//! new benchmarks
class DuckDBBenchmark : public Benchmark {
public:
	DuckDBBenchmark(bool register_benchmark, string name, string group) : Benchmark(register_benchmark, name, group) {
	}
	virtual ~DuckDBBenchmark() {
	}

	//! Load data into DuckDB
	virtual void Load(DuckDBBenchmarkState *state) = 0;
	//! A single query to run against the database
	virtual string GetQuery() {
		return string();
	}
	//! Run a bunch of queries, only called if GetQuery() returns an empty string
	virtual void RunBenchmark(DuckDBBenchmarkState *state) {
	}
	//! This function gets called after the GetQuery() method
	virtual void Cleanup(DuckDBBenchmarkState *state){};
	//! Verify a result
	virtual string VerifyResult(QueryResult *result) = 0;
	//! Whether or not the benchmark is performed on an in-memory database
	virtual bool InMemory() {
		return true;
	}

	string GetDatabasePath() {
		if (!InMemory()) {
			string path = "duckdb_benchmark_db.db";
			DeleteDatabase(path);
			return path;
		} else {
			return string();
		}
	}

	virtual unique_ptr<DuckDBBenchmarkState> CreateBenchmarkState() {
		return make_unique<DuckDBBenchmarkState>(GetDatabasePath());
	}

	unique_ptr<BenchmarkState> Initialize() override {
		auto state = CreateBenchmarkState();
		Load(state.get());
		return move(state);
	}

	void Run(BenchmarkState *state_) override {
		auto state = (DuckDBBenchmarkState *)state_;
		string query = GetQuery();
		if (query.empty()) {
			RunBenchmark(state);
		} else {
			state->result = state->conn.Query(query);
		}
	}

	void Cleanup(BenchmarkState *state_) override {
		auto state = (DuckDBBenchmarkState *)state_;
		Cleanup(state);
	}

	string Verify(BenchmarkState *state_) override {
		auto state = (DuckDBBenchmarkState *)state_;
		return VerifyResult(state->result.get());
	}

	string GetLogOutput(BenchmarkState *state_) override {
		auto state = (DuckDBBenchmarkState *)state_;
		return state->conn.context->profiler.ToJSON();
	}

	//! Interrupt the benchmark because of a timeout
	void Interrupt(BenchmarkState *state_) override {
		auto state = (DuckDBBenchmarkState *)state_;
		state->conn.Interrupt();
	}
};

} // namespace duckdb
