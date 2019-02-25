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

namespace duckdb {

//! Represents a physical ordering of the data. Note that this will not change
//! the data but only add a selection vector.
class PhysicalOrder : public PhysicalOperator {
public:
	PhysicalOrder(LogicalOperator &op, OrderByDescription description)
	    : PhysicalOperator(PhysicalOperatorType::ORDER_BY, op.types), description(std::move(description)) {
	}

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	void AcceptExpressions(SQLNodeVisitor *v) override {
		for (auto &odesc : description.orders) {
			v->VisitExpression(&odesc.expression);
		}
	};

	OrderByDescription description;
};

class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderOperatorState(PhysicalOperator *child)
	    : PhysicalOperatorState(child), position(0) {
	}

	size_t position;
	ChunkCollection sorted_data;
	unique_ptr<uint64_t[]> sorted_vector;
};
} // namespace duckdb
