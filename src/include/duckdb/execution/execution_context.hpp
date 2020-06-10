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
#include <queue>

namespace duckdb {
class ClientContext;
class DataChunk;
class PhysicalOperator;
class PhysicalOperatorState;

class ExecutionContext {
	friend class Pipeline;
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

	std::queue<Pipeline*> scheduled_pipelines;
private:
	ClientContext &context;
	unique_ptr<PhysicalOperator> physical_plan;
	unique_ptr<PhysicalOperatorState> physical_state;
	vector<unique_ptr<Pipeline>> pipelines;
	unordered_map<ChunkCollection*, Pipeline*> delim_join_dependencies;
};
} // namespace duckdb
