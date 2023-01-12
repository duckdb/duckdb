//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

class Executor;
class Event;
class MetaPipeline;

class PipelineBuildState {
public:
	//! How much to increment batch indexes when multiple pipelines share the same source
	constexpr static idx_t BATCH_INCREMENT = 10000000000000;

public:
	//! Duplicate eliminated join scan dependencies
	unordered_map<PhysicalOperator *, Pipeline *> delim_join_dependencies;

public:
	void SetPipelineSource(Pipeline &pipeline, PhysicalOperator *op);
	void SetPipelineSink(Pipeline &pipeline, PhysicalOperator *op, idx_t sink_pipeline_count);
	void SetPipelineOperators(Pipeline &pipeline, vector<PhysicalOperator *> operators);
	void AddPipelineOperator(Pipeline &pipeline, PhysicalOperator *op);
	shared_ptr<Pipeline> CreateChildPipeline(Executor &executor, Pipeline &pipeline, PhysicalOperator *op);

	PhysicalOperator *GetPipelineSource(Pipeline &pipeline);
	PhysicalOperator *GetPipelineSink(Pipeline &pipeline);
	vector<PhysicalOperator *> GetPipelineOperators(Pipeline &pipeline);
};

//! The Pipeline class represents an execution pipeline starting at a
class Pipeline : public std::enable_shared_from_this<Pipeline> {
	friend class Executor;
	friend class PipelineExecutor;
	friend class PipelineEvent;
	friend class PipelineFinishEvent;
	friend class PipelineBuildState;
	friend class MetaPipeline;

public:
	explicit Pipeline(Executor &execution_context);

	Executor &executor;

public:
	ClientContext &GetClientContext();

	void AddDependency(shared_ptr<Pipeline> &pipeline);

	void Ready();
	void Reset();
	void ResetSink();
	void ResetSource(bool force);
	void ClearSource() {
		source_state.reset();
	}
	void Schedule(shared_ptr<Event> &event);

	//! Finalize this pipeline
	void Finalize(Event &event);

	string ToString() const;
	void Print() const;
	void PrintDependencies() const;

	//! Returns query progress
	bool GetProgress(double &current_percentage, idx_t &estimated_cardinality);

	//! Returns a list of all operators (including source and sink) involved in this pipeline
	vector<PhysicalOperator *> GetOperators() const;

	PhysicalOperator *GetSink() {
		return sink;
	}

	PhysicalOperator *GetSource() {
		return source;
	}

	//! Returns whether any of the operators in the pipeline care about preserving insertion order
	bool IsOrderDependent() const;

private:
	//! Whether or not the pipeline has been readied
	bool ready;
	//! Whether or not the pipeline has been initialized
	atomic<bool> initialized;
	//! The source of this pipeline
	PhysicalOperator *source = nullptr;
	//! The chain of intermediate operators
	vector<PhysicalOperator *> operators;
	//! The sink (i.e. destination) for data; this is e.g. a hash table to-be-built
	PhysicalOperator *sink = nullptr;

	//! The global source state
	unique_ptr<GlobalSourceState> source_state;

	//! The parent pipelines (i.e. pipelines that are dependent on this pipeline to finish)
	vector<weak_ptr<Pipeline>> parents;
	//! The dependencies of this pipeline
	vector<weak_ptr<Pipeline>> dependencies;

	//! The base batch index of this pipeline
	idx_t base_batch_index = 0;

private:
	void ScheduleSequentialTask(shared_ptr<Event> &event);
	bool LaunchScanTasks(shared_ptr<Event> &event, idx_t max_threads);

	bool ScheduleParallel(shared_ptr<Event> &event);
};

} // namespace duckdb
