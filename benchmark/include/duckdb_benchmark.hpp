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

namespace duckdb {

#define DUCKDB_BENCHMARK(NAME, GROUP)                                                                                  \
	class NAME##Benchmark : public DuckDBBenchmark {                                                                   \
		NAME##Benchmark() : DuckDBBenchmark("" #NAME, GROUP) {                                                         \
		}                                                                                                              \
                                                                                                                       \
	public:                                                                                                            \
		static NAME##Benchmark *GetInstance() {                                                                        \
			static NAME##Benchmark singleton;                                                                          \
			return &singleton;                                                                                         \
		}

#define FINISH_BENCHMARK(NAME)                                                                                         \
	}                                                                                                                  \
	;                                                                                                                  \
	auto global_instance_##NAME = NAME##Benchmark::GetInstance();

//! Base class for any state that has to be kept by a Benchmark
struct DuckDBBenchmarkState : public BenchmarkState {
	DuckDB db;
	DuckDBConnection conn;
	unique_ptr<DuckDBResult> result;

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
	DuckDBBenchmark(string name, string group) : Benchmark(name, group) {
	}

	//! Load data into DuckDB
	virtual void Load(DuckDBBenchmarkState *state) = 0;
	//! Run queries against the DB
	virtual string GetQuery() = 0;
	//! This function gets called after the GetQuery() method
	virtual void Cleanup(DuckDBBenchmarkState *state){};
	//! Verify a result
	virtual string VerifyResult(DuckDBResult *result) = 0;

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
		return state->conn.context.profiler.ToJSON();
	}

	//! Interrupt the benchmark because of a timeout
	void Interrupt(BenchmarkState *state_) override {
		auto state = (DuckDBBenchmarkState *)state_;
		state->conn.Interrupt();
	}
};

} // namespace duckdb
