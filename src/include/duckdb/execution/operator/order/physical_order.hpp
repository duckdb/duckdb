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
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

//! Represents a physical ordering of the data. Note that this will not change
//! the data but only add a selection vector.
class PhysicalOrder : public PhysicalOperator {
public:
	PhysicalOrder(vector<TypeId> types, vector<BoundOrderByNode> orders)
	    : PhysicalOperator(PhysicalOperatorType::ORDER_BY, move(types)), orders(move(orders)) {
	}

	vector<BoundOrderByNode> orders;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

} // namespace duckdb
