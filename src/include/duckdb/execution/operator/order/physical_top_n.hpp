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
class PhysicalTopN : public PhysicalSink {
public:
	PhysicalTopN(LogicalOperator &op, vector<BoundOrderByNode> orders, idx_t limit, idx_t offset)
	    : PhysicalSink(PhysicalOperatorType::TOP_N, op.types), orders(move(orders)), limit(limit), offset(offset) {
	}

	vector<BoundOrderByNode> orders;
	idx_t limit;
	idx_t offset;
public:
	void Sink(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) override;
	void Combine(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate) override;
	void Finalize(ClientContext &context, GlobalOperatorState &state) override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ClientContext &context) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

private:
	unique_ptr<idx_t[]> ComputeTopN(ChunkCollection &big_data, idx_t &heap_size);
};

} // namespace duckdb
