//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/meta_pipeline.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalRecursiveCTE;

//! MetaPipeline represents a set of pipelines that all have the same sink
class MetaPipeline : public std::enable_shared_from_this<MetaPipeline> {
	//! We follow these rules when building:
	//! 1. For joins, build out the blocking side before going down the probe side
	//!     - The current streaming pipeline will have a dependency on it (dependency across MetaPipelines)
	//!     - Unions of this streaming pipeline will automatically inherit this dependency
	//! 2. Build child pipelines last (e.g., Hash Join becomes source after probe is done: scan HT for FULL OUTER JOIN)
	//!     - 'last' means after building out all other pipelines associated with this operator
	//!     - The child pipeline automatically has dependencies (within this MetaPipeline) on:
	//!         * The 'current' streaming pipeline
	//!         * And all pipelines that were added to the MetaPipeline after 'current'
public:
	//! Create a MetaPipeline with the given sink
	explicit MetaPipeline(Executor &executor, PipelineBuildState &state, PhysicalOperator *sink);

public:
	//! Get the Executor for this MetaPipeline
	Executor &GetExecutor() const;
	//! Get the PipelineBuildState for this MetaPipeline
	PipelineBuildState &GetState() const;
	//! Get the sink operator for this MetaPipeline
	PhysicalOperator *GetSink() const;

	//! Get the initial pipeline of this MetaPipeline
	shared_ptr<Pipeline> &GetBasePipeline();
	//! Get the pipelines of this MetaPipeline
	void GetPipelines(vector<shared_ptr<Pipeline>> &result, bool recursive);
	//! Get the MetaPipeline children of this MetaPipeline
	void GetMetaPipelines(vector<shared_ptr<MetaPipeline>> &result, bool recursive, bool skip);
	//! Get the dependencies (within this MetaPipeline) of the given Pipeline
	const vector<Pipeline *> *GetDependencies(Pipeline *dependant) const;
	//! Whether this MetaPipeline has a recursive CTE
	bool HasRecursiveCTE() const;
	//! Assign a batch index to the given pipeline
	void AssignNextBatchIndex(Pipeline *pipeline);
	//! Let 'dependant' depend on all pipeline that were created since 'start',
	//! where 'including' determines whether 'start' is added to the dependencies
	void AddDependenciesFrom(Pipeline *dependant, Pipeline *start, bool including);
	//! Make sure that the given pipeline has its own PipelineFinishEvent (e.g., for IEJoin - double Finalize)
	void AddFinishEvent(Pipeline *pipeline);
	//! Whether the pipeline needs its own PipelineFinishEvent
	bool HasFinishEvent(Pipeline *pipeline);

public:
	//! Build the MetaPipeline with 'op' as the first operator (excl. the shared sink)
	void Build(PhysicalOperator *op);
	//! Ready all the pipelines (recursively)
	void Ready();

	//! Create an empty pipeline within this MetaPipeline
	Pipeline *CreatePipeline();
	//! Create a union pipeline (clone of 'current')
	Pipeline *CreateUnionPipeline(Pipeline &current, bool order_matters);
	//! Create a child pipeline op 'current' starting at 'op',
	//! where 'last_pipeline' is the last pipeline added before building out 'current'
	void CreateChildPipeline(Pipeline &current, PhysicalOperator *op, Pipeline *last_pipeline);
	//! Create a MetaPipeline child that 'current' depends on
	MetaPipeline *CreateChildMetaPipeline(Pipeline &current, PhysicalOperator *op);

private:
	//! The executor for all MetaPipelines in the query plan
	Executor &executor;
	//! The PipelineBuildState for all MetaPipelines in the query plan
	PipelineBuildState &state;
	//! The sink of all pipelines within this MetaPipeline
	PhysicalOperator *sink;
	//! The recursive CTE of this MetaPipeline (if any)
	PhysicalRecursiveCTE *recursive_cte = nullptr;
	//! All pipelines with a different source, but the same sink
	vector<shared_ptr<Pipeline>> pipelines;
	//! The pipelines that must finish before the MetaPipeline is finished
	vector<Pipeline *> final_pipelines;
	//! Dependencies within this MetaPipeline
	unordered_map<Pipeline *, vector<Pipeline *>> dependencies;
	//! Other MetaPipelines that this MetaPipeline depends on
	vector<shared_ptr<MetaPipeline>> children;
	//! Next batch index
	idx_t next_batch_index;
	//! Pipelines (other than the base pipeline) that need their own PipelineFinishEvent (e.g., for IEJoin)
	unordered_set<Pipeline *> finish_pipelines;
};

} // namespace duckdb
