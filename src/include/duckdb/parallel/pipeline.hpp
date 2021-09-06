//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {
class Executor;

//! The Pipeline class represents an execution pipeline
class Pipeline : public std::enable_shared_from_this<Pipeline> {
	friend class Executor;
	friend class PipelineExecutor;

public:
	Pipeline(Executor &execution_context, ProducerToken &token);

	Executor &executor;
	ProducerToken &token;

public:
	ClientContext &GetClientContext();

	void AddDependency(shared_ptr<Pipeline> &pipeline);
	void CompleteDependency();
	bool HasDependencies() {
		return !dependencies.empty();
	}

	void Ready();
	void Reset();
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
	idx_t source_idx = 0;
	//! The chain of intermediate operators
	vector<PhysicalOperator *> operators;
	//! The sink (i.e. destination) for data; this is e.g. a hash table to-be-built
	PhysicalOperator *sink;

	//! The global source state
	unique_ptr<GlobalSourceState> source_state;
	//! The global sink state
	unique_ptr<GlobalSinkState> sink_state;

	//! The parent pipelines (i.e. pipelines that are dependent on this pipeline to finish)
	unordered_map<Pipeline *, weak_ptr<Pipeline>> parents;
	//! The dependencies of this pipeline
	unordered_map<Pipeline *, weak_ptr<Pipeline>> dependencies;
	//! The amount of completed dependencies (the pipeline can only be started after the dependencies have finished
	//! executing)
	atomic<idx_t> finished_dependencies;

	//! Whether or not the pipeline is finished executing
	bool finished;
	//! The recursive CTE node that this pipeline belongs to, and may be executed multiple times
	PhysicalOperator *recursive_cte;

private:
	bool GetProgress(ClientContext &context, PhysicalOperator *op, int &current_percentage);
	void ScheduleSequentialTask();
	bool LaunchScanTasks(idx_t max_threads);

	bool ScheduleParallel();
};

} // namespace duckdb
