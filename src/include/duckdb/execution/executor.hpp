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

#include <queue>

namespace duckdb {
class ClientContext;
class DataChunk;
class PhysicalOperator;
class PipelineExecutor;
class OperatorState;
class ThreadContext;
class Task;

struct PipelineEventStack;
struct ProducerToken;

class Executor {
	friend class Pipeline;
	friend class PipelineTask;

public:
	explicit Executor(ClientContext &context);
	~Executor();

	ClientContext &context;

public:
	void Initialize(PhysicalOperator *physical_plan);
	void BuildPipelines(PhysicalOperator *op, Pipeline *current);

	void Reset();

	vector<LogicalType> GetTypes();

	unique_ptr<DataChunk> FetchChunk();

	//! Push a new error
	void PushError(const string &exception);
	bool GetError(string &exception);

	//! Flush a thread context into the client context
	void Flush(ThreadContext &context);

	//! Returns the progress of the pipelines
	bool GetPipelinesProgress(int &current_progress);

	void CompletePipeline() {
		completed_pipelines++;
	}
	ProducerToken &GetToken() {
		return *producer;
	}
	void AddEvent(shared_ptr<Event> event);

private:
	void ScheduleEvents();
	void SchedulePipeline(const shared_ptr<Pipeline> &pipeline, unordered_map<Pipeline *, PipelineEventStack> &event_map);
	void ScheduleUnionPipeline(const shared_ptr<Pipeline> &pipeline, PipelineEventStack &stack, unordered_map<Pipeline *, PipelineEventStack> &event_map);
	void ScheduleChildPipeline(const shared_ptr<Pipeline> &pipeline, unordered_map<Pipeline *, PipelineEventStack> &event_map);
	void ExtractPipelines(shared_ptr<Pipeline> &pipeline, vector<shared_ptr<Pipeline>> &result);
	bool NextExecutor();

	void AddChildPipeline(Pipeline *current);

	void VerifyPipeline(Pipeline &pipeline);
	void VerifyPipelines();
private:
	PhysicalOperator *physical_plan;

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
	vector<string> exceptions;
	//! List of events
	vector<shared_ptr<Event>> events;

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

	//! Duplicate eliminated join scan dependencies
	unordered_map<PhysicalOperator *, Pipeline *> delim_join_dependencies;

	//! Active recursive CTE node (if any)
	PhysicalOperator *recursive_cte;
};
} // namespace duckdb
