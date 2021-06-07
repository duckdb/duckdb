//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/order/physical_top_n.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_sink.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

//! Represents a physical ordering of the data. Note that this will not change
//! the data but only add a selection vector.
class PhysicalTopN : public PhysicalSink {
public:
	PhysicalTopN(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t limit, idx_t offset,
	             idx_t estimated_cardinality)
	    : PhysicalSink(PhysicalOperatorType::TOP_N, move(types), estimated_cardinality), orders(move(orders)),
	      limit(limit), offset(offset) {
	}

	vector<BoundOrderByNode> orders;
	idx_t limit;
	idx_t offset;

public:
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate) override;
	bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate) override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	string ParamsToString() const override;

private:
	unique_ptr<idx_t[]> ComputeTopN(ChunkCollection &big_data, idx_t &heap_size);
};

} // namespace duckdb
