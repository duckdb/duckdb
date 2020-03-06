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
	unique_ptr<SinkState> sink_state;
	//! The sink (i.e. destination) for data; this is e.g. a hash table to-be-built
	PhysicalSink *sink;
	//! The parent pipeline (i.e. the pipeline that is dependent on this pipeline to finish)
	Pipeline *parent;
	//! The dependents of this pipeline
	vector<unique_ptr<Pipeline>> dependents;
public:
	//! Execute the pipeline sequentially on a single thread
	void Execute(ClientContext &context);

	void EraseDependent(Pipeline *pipeline);
	//! Finish executing this pipeline
	void Finish();
};

} // namespace duckdb
