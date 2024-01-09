//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/order/physical_top_n.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

//! Represents a physical ordering of the data. Note that this will not change
//! the data but only add a selection vector.
class PhysicalTopN : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::TOP_N;

public:
	PhysicalTopN(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t limit, idx_t offset,
	             idx_t estimated_cardinality);

	vector<BoundOrderByNode> orders;
	idx_t limit;
	idx_t offset;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	OrderPreservationType SourceOrder() const override {
		return OrderPreservationType::FIXED_ORDER;
	}

public:
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

	string ParamsToString() const override;
};

} // namespace duckdb
