//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/order/physical_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_sink.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

class OrderGlobalState;

//! Physically re-orders the input data
class PhysicalOrder : public PhysicalSink {
public:
	PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders,
	              vector<unique_ptr<BaseStatistics>> statistics, idx_t estimated_cardinality);

	//! Input data
	vector<BoundOrderByNode> orders;
	//! Statistics of the order expressions
	vector<unique_ptr<BaseStatistics>> statistics;

public:
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	void Sink(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p,
	          DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p) override;
	bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate_p) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;

	string ParamsToString() const override;

	//! Schedules tasks to merge the data during the Finalize phase
	static void ScheduleMergeTasks(Pipeline &pipeline, ClientContext &context, OrderGlobalState &state);
};

} // namespace duckdb
