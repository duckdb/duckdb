//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/pending_execution_result.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/execution/task_error_manager.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include <condition_variable>

namespace duckdb {
class ClientContext;
class DataChunk;
class PhysicalOperator;
class PipelineExecutor;
class OperatorState;
class QueryProfiler;
class ThreadContext;
class Task;

struct PipelineEventStack;
struct ProducerToken;
struct ScheduleEventData;

class Executor {
	friend class Pipeline;
	friend class PipelineTask;
	friend class PipelineBuildState;

public:
	explicit Executor(ClientContext &context);
	~Executor();

	ClientContext &context;

public:
	static Executor &Get(ClientContext &context);

	void Initialize(PhysicalOperator &physical_plan);
	void Initialize(unique_ptr<PhysicalOperator> physical_plan);

	void CancelTasks();
	PendingExecutionResult ExecuteTask(bool dry_run = false);
	void WaitForTask();
	void SignalTaskRescheduled(lock_guard<mutex> &);

	void Reset();

	vector<LogicalType> GetTypes();

	//! Push a new error
	void PushError(ErrorData exception);

	ErrorData GetError();

	//! True if an error has been thrown
	bool HasError();
	//! Throw the exception that was pushed using PushError.
	//! Should only be called if HasError returns true
	void ThrowException();

	//! Work on tasks for this specific executor, until there are no tasks remaining
	void WorkOnTasks();

	//! Flush a thread context into the client context
	void Flush(ThreadContext &context);

	//! Reschedules a task that was blocked
	void RescheduleTask(shared_ptr<Task> &task);

	//! Add the task to be rescheduled
	void AddToBeRescheduled(shared_ptr<Task> &task);

	//! Returns the progress of the pipelines
	bool GetPipelinesProgress(double &current_progress, uint64_t &current_cardinality, uint64_t &total_cardinality);

	void CompletePipeline() {
		completed_pipelines++;
	}
	ProducerToken &GetToken() {
		return *producer;
	}
	void AddEvent(shared_ptr<Event> event);

	void AddRecursiveCTE(PhysicalOperator &rec_cte);
	void ReschedulePipelines(const vector<shared_ptr<MetaPipeline>> &pipelines, vector<shared_ptr<Event>> &events);

	//! Whether or not the root of the pipeline is a result collector object
	bool HasResultCollector();
	//! Whether or not the root of the pipeline is a streaming result collector object
	bool HasStreamingResultCollector();
	//! Returns the query result - can only be used if `HasResultCollector` returns true
	unique_ptr<QueryResult> GetResult();

	//! Returns true if all pipelines have been completed
	bool ExecutionIsFinished();

	void RegisterTask() {
		executor_tasks++;
	}
	void UnregisterTask() {
		executor_tasks--;
	}

private:
	//! Check if the streaming query result is waiting to be fetched from, must hold the 'executor_lock'
	bool ResultCollectorIsBlocked();
	void InitializeInternal(PhysicalOperator &physical_plan);

	void ScheduleEvents(const vector<shared_ptr<MetaPipeline>> &meta_pipelines);
	static void ScheduleEventsInternal(ScheduleEventData &event_data);

	static void VerifyScheduledEvents(const ScheduleEventData &event_data);
	static void VerifyScheduledEventsInternal(const idx_t i, const vector<reference<Event>> &vertices,
	                                          vector<bool> &visited, vector<bool> &recursion_stack);

	static void SchedulePipeline(const shared_ptr<MetaPipeline> &pipeline, ScheduleEventData &event_data);

	bool NextExecutor();

	shared_ptr<Pipeline> CreateChildPipeline(Pipeline &current, PhysicalOperator &op);

	void VerifyPipeline(Pipeline &pipeline);
	void VerifyPipelines();

private:
	optional_ptr<PhysicalOperator> physical_plan;
	unique_ptr<PhysicalOperator> owned_plan;

	mutex executor_lock;
	//! All pipelines of the query plan
	vector<shared_ptr<Pipeline>> pipelines;
	//! The root pipelines of the query
	vector<shared_ptr<Pipeline>> root_pipelines;
	//! The recursive CTE's in this query plan
	vector<reference<PhysicalOperator>> recursive_ctes;
	//! The pipeline executor for the root pipeline
	unique_ptr<PipelineExecutor> root_executor;
	//! The current root pipeline index
	idx_t root_pipeline_idx;
	//! The producer of this query
	unique_ptr<ProducerToken> producer;
	//! List of events
	vector<shared_ptr<Event>> events;
	//! The query profiler
	shared_ptr<QueryProfiler> profiler;
	//! Task error manager
	TaskErrorManager error_manager;

	//! The amount of completed pipelines of the query
	atomic<idx_t> completed_pipelines;
	//! The total amount of pipelines in the query
	idx_t total_pipelines;
	//! Whether or not execution is cancelled
	bool cancelled;

	//! The last pending execution result (if any)
	PendingExecutionResult execution_result;
	//! The current task in process (if any)
	shared_ptr<Task> task;

	//! Task that have been descheduled
	unordered_map<Task *, shared_ptr<Task>> to_be_rescheduled_tasks;
	//! The semaphore to signal task rescheduling
	std::condition_variable task_reschedule;

	//! Currently alive executor tasks
	atomic<idx_t> executor_tasks;
};
} // namespace duckdb
