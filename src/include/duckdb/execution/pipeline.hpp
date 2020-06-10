//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/pipeline.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class ExecutionContext;

//! The Pipeline class represents an execution pipeline
class Pipeline {
public:
	Pipeline(ExecutionContext &execution_context);

	ExecutionContext &execution_context;
	//! The child from which to pull chunks
	PhysicalOperator *child;
	//! The global sink state
	unique_ptr<GlobalOperatorState> sink_state;
	//! The sink (i.e. destination) for data; this is e.g. a hash table to-be-built
	PhysicalSink *sink;
	//! The parent pipelines (i.e. pipelines that are dependent on this pipeline to finish)
	unordered_set<Pipeline*> parents;
	//! The dependencies of this pipeline (the pipeline can only be started after the dependencies have finished executing)
	unordered_set<Pipeline*> dependencies;
public:
	//! Execute the pipeline sequentially on a single thread
	void Execute(ClientContext &context);

	void AddDependency(Pipeline *pipeline);
	void EraseDependency(Pipeline *pipeline);
	//! Finish executing this pipeline
	void Finish();

	string ToString() const;
	void Print() const;
};

} // namespace duckdb
