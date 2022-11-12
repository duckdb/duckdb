//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/order/physical_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

class OrderGlobalState;

//! Physically re-orders the input data
class PhysicalOrder : public PhysicalOperator {
public:
	PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, vector<idx_t> projections,
	              idx_t estimated_cardinality);

	//! Input data
	vector<BoundOrderByNode> orders;
	vector<idx_t> projections;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

	bool IsOrderPreserving() const override {
		return false;
	}

public:
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
	                    DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          GlobalSinkState &gstate) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

	string ParamsToString() const override;

	//! Schedules tasks to merge the data during the Finalize phase
	static void ScheduleMergeTasks(Pipeline &pipeline, Event &event, OrderGlobalState &state);
};

} // namespace duckdb
