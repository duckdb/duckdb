//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_sink.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {
class Executor;
class TaskContext;

//! The Pipeline class represents an execution pipeline
class Pipeline {
	friend class Executor;

public:
	Pipeline(Executor &execution_context, ProducerToken &token);

	Executor &executor;
	ProducerToken &token;

public:
	//! Execute a task within the pipeline on a single thread
	void Execute(TaskContext &task);

	void AddDependency(Pipeline *pipeline);
	void CompleteDependency();
	bool HasDependencies() {
		return !dependencies.empty();
	}

	void Reset(ClientContext &context);
	void Schedule();

	//! Finish a single task of this pipeline
	void FinishTask();
	//! Finish executing this pipeline
	void Finish();

	string ToString() const;
	void Print() const;

	void SetRecursiveCTE(PhysicalOperator *op) {
		this->recursive_cte = op;
	}
	PhysicalOperator *GetRecursiveCTE() {
		return recursive_cte;
	}
	unordered_set<Pipeline *> &GetDependencies() {
		return dependencies;
	}
	void ClearParents();

	void IncrementTasks(idx_t amount) {
		this->total_tasks += amount;
	}

	bool IsFinished() {
		return finished;
	}
	//! Returns query progress
	bool GetProgress(int &current_percentage);

public:
	//! The current threads working on the pipeline
	atomic<idx_t> finished_tasks;
	//! The maximum amount of threads that can work on the pipeline
	atomic<idx_t> total_tasks;

private:
	//! The child from which to pull chunks
	PhysicalOperator *child;
	//! The global sink state
	unique_ptr<GlobalOperatorState> sink_state;
	//! The sink (i.e. destination) for data; this is e.g. a hash table to-be-built
	PhysicalSink *sink;
	//! The parent pipelines (i.e. pipelines that are dependent on this pipeline to finish)
	unordered_set<Pipeline *> parents;
	//! The dependencies of this pipeline
	unordered_set<Pipeline *> dependencies;
	//! The amount of completed dependencies (the pipeline can only be started after the dependencies have finished
	//! executing)
	atomic<idx_t> finished_dependencies;

	//! The parallel operator (if any)
	PhysicalOperator *parallel_node;
	//! The parallel state (if any)
	unique_ptr<ParallelState> parallel_state;

	//! Whether or not the pipeline is finished executing
	bool finished;
	//! The recursive CTE node that this pipeline belongs to, and may be executed multiple times
	PhysicalOperator *recursive_cte;

private:
	bool GetProgress(ClientContext &context, PhysicalOperator *op, int &current_percentage);
	void ScheduleSequentialTask();
	bool LaunchScanTasks(PhysicalOperator *op, idx_t max_threads, unique_ptr<ParallelState> parallel_state);
	bool ScheduleOperator(PhysicalOperator *op);
};

} // namespace duckdb
