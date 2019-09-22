//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/order/physical_order_limit.hpp
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
class PhysicalOrderAndLimit : public PhysicalOperator {
public:
	PhysicalOrderAndLimit(LogicalOperator &op, vector<BoundOrderByNode> orders, index_t limit, index_t offset)
	    : PhysicalOperator(PhysicalOperatorType::ORDER_BY_LIMIT, op.types), orders(move(orders)), limit(limit),
	      offset(offset), heap_size(0) {
	}

	vector<BoundOrderByNode> orders;
	index_t limit;
	index_t offset;
	index_t heap_size;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	void CalculateHeapSize(index_t rows);
};

class PhysicalOrderAndLimitOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderAndLimitOperatorState(PhysicalOperator *child) : PhysicalOperatorState(child), position(0) {
	}

	index_t position;
	index_t current_offset;
	ChunkCollection sorted_data;
	unique_ptr<index_t[]> heap;
};
} // namespace duckdb
