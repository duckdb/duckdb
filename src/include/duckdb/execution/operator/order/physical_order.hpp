//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/order/physical_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/row_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/physical_sink.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

struct ContinuousBlock;

//! Represents a physical ordering of the data. Note that this will not change
//! the data but only add a selection vector.
class PhysicalOrder : public PhysicalSink {
public:
	PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders);

	//! Input data
	vector<BoundOrderByNode> orders;

public:
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p,
	          DataChunk &input) override;
	void Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p) override;
	void Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) override;

	static void ScheduleMergeTasks(Pipeline &pipeline, ClientContext &context, GlobalOperatorState &state);
    static void ScheduleMergePathTasks(Pipeline &pipeline, ClientContext &context, GlobalOperatorState &state);

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	string ParamsToString() const override;

private:
	//! 2 << 17 (256KB)
	const static idx_t SORTING_BLOCK_SIZE = 2097152;
};

} // namespace duckdb
