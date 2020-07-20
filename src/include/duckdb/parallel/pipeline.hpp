//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_sink.hpp"

#include <atomic>

namespace duckdb {
class Executor;
class TaskContext;

//! The Pipeline class represents an execution pipeline
class Pipeline {
	friend class Executor;

public:
	Pipeline(Executor &execution_context);

	Executor &executor;

public:
	//! Execute a task within the pipeline on a single thread
	void Execute(TaskContext &task);

	void AddDependency(Pipeline *pipeline);
	void CompleteDependency();
	bool HasDependencies() {
		return dependencies.size() != 0;
	}

	void Schedule();

	//! Finish a single task of this pipeline
	void FinishTask();
	//! Finish executing this pipeline
	void Finish();

	string ToString() const;
	void Print() const;

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
	std::atomic<idx_t> finished_dependencies;

	//! Whether or not the pipeline is finished executing
	bool finished;
	//! The current threads working on the pipeline
	std::atomic<idx_t> finished_tasks;
	//! The maximum amount of threads that can work on the pipeline
	idx_t total_tasks;

private:
	void ScheduleSequentialTask();
	bool ScheduleOperator(PhysicalOperator *op);
};

} // namespace duckdb
