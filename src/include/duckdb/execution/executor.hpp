//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/enums/pending_execution_result.hpp"

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

using event_map_t = unordered_map<const Pipeline *, PipelineEventStack>;

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

	void Initialize(PhysicalOperator *physical_plan);
	void Initialize(unique_ptr<PhysicalOperator> physical_plan);

	void CancelTasks();
	PendingExecutionResult ExecuteTask();

	void Reset();

	vector<LogicalType> GetTypes();

	unique_ptr<DataChunk> FetchChunk();

	//! Push a new error
	void PushError(ExceptionType type, const string &exception);
	//! True if an error has been thrown
	bool HasError();
	//! Throw the exception that was pushed using PushError.
	//! Should only be called if HasError returns true
	void ThrowException();

	//! Work on tasks for this specific executor, until there are no tasks remaining
	void WorkOnTasks();

	//! Flush a thread context into the client context
	void Flush(ThreadContext &context);

	//! Returns the progress of the pipelines
	bool GetPipelinesProgress(double &current_progress);

	void CompletePipeline() {
		completed_pipelines++;
	}
	ProducerToken &GetToken() {
		return *producer;
	}
	void AddEvent(shared_ptr<Event> event);

	void ReschedulePipelines(const vector<shared_ptr<Pipeline>> &pipelines, vector<shared_ptr<Event>> &events);

	//! Whether or not the root of the pipeline is a result collector object
	bool HasResultCollector();
	//! Returns the query result - can only be used if `HasResultCollector` returns true
	unique_ptr<QueryResult> GetResult();

private:
	void InitializeInternal(PhysicalOperator *physical_plan);

	void ScheduleEvents();
	void ScheduleEventsInternal(const vector<shared_ptr<Pipeline>> &pipelines,
	                            unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> &child_pipelines,
	                            vector<shared_ptr<Event>> &events, bool main_schedule = true);

	void SchedulePipeline(const shared_ptr<Pipeline> &pipeline, event_map_t &event_map,
	                      vector<shared_ptr<Event>> &events, bool complete_pipeline);
	Pipeline *ScheduleUnionPipeline(const shared_ptr<Pipeline> &pipeline, const Pipeline *parent,
	                                event_map_t &event_map, vector<shared_ptr<Event>> &events);
	void ScheduleChildPipeline(Pipeline *parent, const shared_ptr<Pipeline> &pipeline, event_map_t &event_map,
	                           vector<shared_ptr<Event>> &events);
	void ExtractPipelines(shared_ptr<Pipeline> &pipeline, vector<shared_ptr<Pipeline>> &result);
	bool NextExecutor();

	void AddChildPipeline(Pipeline *current);

	void VerifyPipeline(Pipeline &pipeline);
	void VerifyPipelines();
	void ThrowExceptionInternal();

private:
	PhysicalOperator *physical_plan;
	unique_ptr<PhysicalOperator> owned_plan;

	mutex executor_lock;
	//! The pipelines of the current query
	vector<shared_ptr<Pipeline>> pipelines;
	//! The root pipeline of the query
	vector<shared_ptr<Pipeline>> root_pipelines;
	//! The pipeline executor for the root pipeline
	unique_ptr<PipelineExecutor> root_executor;
	//! The current root pipeline index
	idx_t root_pipeline_idx;
	//! The producer of this query
	unique_ptr<ProducerToken> producer;
	//! Exceptions that occurred during the execution of the current query
	vector<pair<ExceptionType, string>> exceptions;
	//! List of events
	vector<shared_ptr<Event>> events;
	//! The query profiler
	shared_ptr<QueryProfiler> profiler;

	//! The amount of completed pipelines of the query
	atomic<idx_t> completed_pipelines;
	//! The total amount of pipelines in the query
	idx_t total_pipelines;

	//! The adjacent union pipelines of each pipeline
	//! Union pipelines have the same sink, but can be run concurrently along with this pipeline
	unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> union_pipelines;
	//! Child pipelines of this pipeline
	//! Like union pipelines, child pipelines share the same sink
	//! Unlike union pipelines, child pipelines should be run AFTER their dependencies are completed
	//! i.e. they should be run after the dependencies are completed, but before finalize is called on the sink
	unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> child_pipelines;
	//! Dependencies of child pipelines
	unordered_map<Pipeline *, vector<Pipeline *>> child_dependencies;

	//! The last pending execution result (if any)
	PendingExecutionResult execution_result;
	//! The current task in process (if any)
	unique_ptr<Task> task;
};
} // namespace duckdb
