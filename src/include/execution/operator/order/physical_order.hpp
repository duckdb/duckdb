//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/order/physical_order.hpp
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/physical_operator.hpp"

namespace duckdb {

//! Represents a physical ordering of the data. Note that this will not change
//! the data but only add a selection vector.
class PhysicalOrder : public PhysicalOperator {
  public:
	PhysicalOrder(OrderByDescription description)
	    : PhysicalOperator(PhysicalOperatorType::ORDER_BY),
	      description(std::move(description)) {
	}

	std::vector<TypeId> GetTypes() override;

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	OrderByDescription description;
};

class PhysicalOrderOperatorState : public PhysicalOperatorState {
  public:
	PhysicalOrderOperatorState(PhysicalOperator *child,
	                           ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(child, parent_executor), position(0) {
	}

	size_t position;
	ChunkCollection sorted_data;
	std::unique_ptr<uint64_t[]> sorted_vector;
};
} // namespace duckdb
