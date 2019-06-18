//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/order/physical_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/physical_operator.hpp"
#include "planner/bound_query_node.hpp"

namespace duckdb {

//! Represents a physical ordering of the data. Note that this will not change
//! the data but only add a selection vector.
class PhysicalOrder : public PhysicalOperator {
public:
	PhysicalOrder(LogicalOperator &op, vector<BoundOrderByNode> orders)
	    : PhysicalOperator(PhysicalOperatorType::ORDER_BY, op.types), orders(move(orders)) {
	}

	vector<BoundOrderByNode> orders;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderOperatorState(PhysicalOperator *child) : PhysicalOperatorState(child), position(0) {
	}

	index_t position;
	ChunkCollection sorted_data;
	unique_ptr<index_t[]> sorted_vector;
};
} // namespace duckdb
