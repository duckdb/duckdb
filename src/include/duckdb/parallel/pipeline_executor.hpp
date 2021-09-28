//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/stack.hpp"

namespace duckdb {
class Executor;

//! The Pipeline class represents an execution pipeline
class PipelineExecutor {
public:
	PipelineExecutor(ClientContext &context, Pipeline &pipeline);

	//! Execute a pipeline with a sink until the source is exhausted
	void Execute();
	//! Execute a pipeline without a sink, and retrieve a single DataChunk
	void Execute(DataChunk &result);

	//! Initializes a chunk with the types that will flow out of the executor
	void InitializeChunk(DataChunk &chunk);

private:
	//! The pipeline to process
	Pipeline &pipeline;
	//! The thread context of this executor
	ThreadContext thread;
	//! The total execution context of this executor
	ExecutionContext context;

	//! Intermediate chunks for the operators
	vector<unique_ptr<DataChunk>> intermediate_chunks;
	//! Intermediate states for the operators
	vector<unique_ptr<OperatorState>> intermediate_states;

	//! The local source state
	unique_ptr<LocalSourceState> local_source_state;
	//! The local sink state (if any)
	unique_ptr<LocalSinkState> local_sink_state;

	//! The final chunk used for moving data into the sink
	DataChunk final_chunk;

	//! The operators that are not yet finished executing and have data remaining
	//! If the stack of in_process_operators is empty, we fetch from the source instead
	stack<idx_t> in_process_operators;

private:
	void StartOperator(PhysicalOperator *op);
	void EndOperator(PhysicalOperator *op, DataChunk *chunk);
	//! Reset the operator index to the first operator
	void GoToSource(idx_t &current_idx);
	void FetchFromSource(DataChunk &result);
};

} // namespace duckdb
