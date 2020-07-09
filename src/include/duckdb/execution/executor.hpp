//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include <queue>

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

	ClientContext &context;
	bool finished;
public:
	void Initialize(unique_ptr<PhysicalOperator> physical_plan);
	void BuildPipelines(PhysicalOperator *op, Pipeline *parent);


	void Work();
	void Reset();

	void SchedulePipeline(shared_ptr<Pipeline> pipeline);
	void ErasePipeline(Pipeline *pipeline);

	vector<TypeId> GetTypes();

	unique_ptr<DataChunk> FetchChunk();

	//! Push a new error
	void PushError(std::string exception);

	//! Flush a thread context into the client context
	void Flush(ThreadContext &context);
private:
	unique_ptr<PhysicalOperator> physical_plan;
	unique_ptr<PhysicalOperatorState> physical_state;

	mutex executor_lock;
	//! The pipelines of the current query
	vector<shared_ptr<Pipeline>> pipelines;
	//! Exceptions that occurred during the execution of the current query
	vector<string> exceptions;

	std::queue<shared_ptr<Pipeline>> scheduled_pipelines;


    unordered_map<ChunkCollection *, Pipeline *> delim_join_dependencies;
};
} // namespace duckdb
