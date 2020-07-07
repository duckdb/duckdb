//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {
class ClientContext;
class DataChunk;
class PhysicalOperator;
class PhysicalOperatorState;
class ThreadContext;

struct ProducerToken;

class Executor {
	friend class Pipeline;
	friend class PipelineTask;

public:
	Executor(ClientContext &context);
	~Executor();

public:
	void Initialize(unique_ptr<PhysicalOperator> physical_plan);
	void BuildPipelines(PhysicalOperator *op, Pipeline *parent);

	void Reset();

	vector<TypeId> GetTypes();

	unique_ptr<DataChunk> FetchChunk();

	//! Push a new error
	void PushError(std::string exception);

	//! Flush a thread context into the client context
	void Flush(ThreadContext &context);
private:
	void Schedule(Pipeline *pipeline);

	void ErasePipeline(Pipeline *pipeline);

private:
	ClientContext &context;
	unique_ptr<PhysicalOperator> physical_plan;
	unique_ptr<PhysicalOperatorState> physical_state;
	std::mutex pipeline_lock;
	//! The pipelines of the current query
	vector<unique_ptr<Pipeline>> pipelines;
	//! The producer token of this query, any tasks created by this query are associated with this producer token
    unique_ptr<ProducerToken> producer;
	//! Exceptions that occurred during the execution of the current query
	vector<string> exceptions;


    unordered_map<ChunkCollection *, Pipeline *> delim_join_dependencies;
};
} // namespace duckdb
