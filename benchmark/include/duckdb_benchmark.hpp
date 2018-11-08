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

#define DUCKDB_BENCHMARK(NAME, GROUP)                                          \
	class NAME##Benchmark : public DuckDBBenchmark {                           \
		NAME##Benchmark() : DuckDBBenchmark("" #NAME, GROUP) {                 \
		}                                                                      \
                                                                               \
	  public:                                                                  \
		static NAME##Benchmark *GetInstance() {                                \
			static NAME##Benchmark singleton;                                  \
			return &singleton;                                                 \
		}

#define FINISH_BENCHMARK(NAME)                                                 \
	}                                                                          \
	;                                                                          \
	auto global_instance_##NAME = NAME##Benchmark::GetInstance();

//! Base class for any state that has to be kept by a Benchmark
struct DuckDBBenchmarkState : public BenchmarkState {
	DuckDB db;
	DuckDBConnection conn;
	std::unique_ptr<DuckDBResult> result;

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
	DuckDBBenchmark(std::string name, std::string group)
	    : Benchmark(name, group) {
	}

	//! Load data into DuckDB
	virtual void Load(DuckDBBenchmarkState *state) = 0;
	//! Run queries against the DB
	virtual std::string GetQuery() = 0;
	//! Verify a result
	virtual std::string VerifyResult(DuckDBResult *result) = 0;

	virtual std::unique_ptr<BenchmarkState> Initialize() override {
		auto state = make_unique<DuckDBBenchmarkState>();
		Load(state.get());
		return move(state);
	}

	virtual void Run(BenchmarkState *state_) override {
		auto state = (DuckDBBenchmarkState *)state_;
		state->result = state->conn.Query(GetQuery());
	}

	virtual std::string Verify(BenchmarkState *state_) override {
		auto state = (DuckDBBenchmarkState *)state_;
		return VerifyResult(state->result.get());
	}

	virtual std::string GetLogOutput(BenchmarkState *state_) override {
		auto state = (DuckDBBenchmarkState *)state_;
		return state->conn.GetProfilingInformation();
	}

	//! Interrupt the benchmark because of a timeout
	virtual void Interrupt(BenchmarkState *state_) override {
		auto state = (DuckDBBenchmarkState *)state_;
		state->conn.Interrupt();
	}
};

} // namespace duckdb
