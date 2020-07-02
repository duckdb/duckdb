//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/execution/pipeline.hpp"

namespace duckdb {
class ClientContext;
class DataChunk;
class PhysicalOperator;
class PhysicalOperatorState;
struct ProducerToken;

class ExecutionContext {
	friend class Pipeline;
	friend class PipelineTask;

public:
	ExecutionContext(ClientContext &context);
	~ExecutionContext();

public:
	void Initialize(unique_ptr<PhysicalOperator> physical_plan);
	void BuildPipelines(PhysicalOperator *op, Pipeline *parent);

	void Reset();

	vector<TypeId> GetTypes();

	unique_ptr<DataChunk> FetchChunk();

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
    unordered_map<ChunkCollection *, Pipeline *> delim_join_dependencies;
};
} // namespace duckdb
