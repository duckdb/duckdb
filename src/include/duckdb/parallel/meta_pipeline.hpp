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

//! MetaPipeline represents a set of pipelines that have the same sink
class MetaPipeline : public std::enable_shared_from_this<MetaPipeline> {
public:
	explicit MetaPipeline(Executor &executor, PipelineBuildState &state, PhysicalOperator *sink);

	Executor &GetExecutor() const;
	PipelineBuildState &GetState() const;
	PhysicalOperator *GetSink() const;

	bool HasRecursiveCTE() const;
	PhysicalRecursiveCTE *GetRecursiveCTE() const;
	void SetRecursiveCTE(PhysicalOperator *recursive_cte);

	shared_ptr<Pipeline> &GetRootPipeline();
	void GetPipelines(vector<shared_ptr<Pipeline>> &result, bool recursive);
	void GetMetaPipelines(vector<shared_ptr<MetaPipeline>> &result, bool recursive);
	vector<shared_ptr<MetaPipeline>> &GetChildren();
	const vector<Pipeline *> *GetDependencies(Pipeline *pipeline) const;
	vector<Pipeline *> &GetFinalPipelines();

public:
	void Build(PhysicalOperator *op);
	void Ready();
	void Reset(ClientContext &context, bool reset_sink);

	Pipeline *CreateUnionPipeline(Pipeline &current);
	Pipeline *CreateChildPipeline(Pipeline &current);
	MetaPipeline *CreateChildMetaPipeline(Pipeline &current, PhysicalOperator *op);

	void AddInterPipelineDependency(Pipeline *dependant, Pipeline *dependee);

private:
	Pipeline *CreatePipeline();
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
	//! Dependencies between the confluent pipelines
	unordered_map<Pipeline *, vector<Pipeline *>> inter_pipeline_dependencies;
	//! Other MetaPipelines that this MetaPipeline depends on
	vector<shared_ptr<MetaPipeline>> children;
};

// class ConfluentPipelineBuildState {
// public:
//	explicit ConfluentPipelineBuildState(MetaPipeline &pipeline_sink);
//
// public:
//	//! The confluent pipelines that the current pipeline belongs to
//	MetaPipeline &pipeline_sink;
//	//! Pipelines with the same sink, that the current pipeline being built may depend on
//	vector<shared_ptr<Pipeline>> sibling_pipelines;
// };

} // namespace duckdb
