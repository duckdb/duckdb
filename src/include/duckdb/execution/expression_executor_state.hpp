//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_executor_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/cycle_counter.hpp"
#include "duckdb/common/random_engine.hpp"


namespace duckdb {
class Expression;
class ExpressionExecutor;
struct ExpressionExecutorState;

struct ExpressionState {
	ExpressionState(Expression &expr, ExpressionExecutorState &root);
	virtual ~ExpressionState() {
	}

	Expression &expr;
	ExpressionExecutorState &root;
	vector<unique_ptr<ExpressionState>> child_states;
	vector<LogicalType> types;
	DataChunk intermediate_chunk;
	string name;
	double time;
	CycleCounter profiler;

public:
	void AddChild(Expression *expr);
	void Finalize();
};

struct ExpressionExecutorState {
	explicit ExpressionExecutorState(const string &name);
	unique_ptr<ExpressionState> root_state;
	ExpressionExecutor *executor;
	CycleCounter profiler;
	string name;
	double time;
    //! Count the number of time the executor called
    uint64_t total_count = 0;
    //! Count the number of time the executor called since last sampling
    uint64_t current_count = 0;
    //! Show the next sample
    uint64_t next_sample = 0;
    //! Count the number of samples
    uint64_t sample_count = 0;
    //! Count the number of tuples in all samples
    uint64_t sample_tuples_count = 0;
    //! Count the number of tuples processed by this executor
    uint64_t tuples_count = 0;
    //! the random number generator used for sampling
    RandomEngine random;

	// Next_sample determines if a sample needs to be take, if so start the profiler
	void TakeSample(){
        if (current_count >= next_sample) {
            profiler.Start();
        }
	}

    // Process the sample
    void ProcessSample(int chunk_size){
        if (current_count >= next_sample) {
            profiler.End();
            time += profiler.Elapsed();
        }
        if (current_count >= next_sample) {
            next_sample = 50 + random.NextRandomInteger() % 100;
            ++sample_count;
            sample_tuples_count += chunk_size;
            current_count = 0;
        } else {
            ++current_count;
        }
        ++total_count;
        tuples_count += chunk_size;
	}
};

} // namespace duckdb