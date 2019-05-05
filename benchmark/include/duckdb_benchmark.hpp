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
#include "main/client_context.hpp"

namespace duckdb {

//! Base class for any state that has to be kept by a Benchmark
struct DuckDBBenchmarkState : public BenchmarkState {
	DuckDB db;
	Connection conn;
	unique_ptr<QueryResult> result;

	DuckDBBenchmarkState() : db(nullptr), conn(db) {
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
	//! Run queries against the DB
	virtual string GetQuery() = 0;
	//! This function gets called after the GetQuery() method
	virtual void Cleanup(DuckDBBenchmarkState *state){};
	//! Verify a result
	virtual string VerifyResult(QueryResult *result) = 0;

	unique_ptr<BenchmarkState> Initialize() override {
		auto state = make_unique<DuckDBBenchmarkState>();
		Load(state.get());
		return move(state);
	}

	void Run(BenchmarkState *state_) override {
		auto state = (DuckDBBenchmarkState *)state_;
		state->result = state->conn.Query(GetQuery());
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
