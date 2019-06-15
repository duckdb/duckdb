//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/join/physical_cross_product.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/physical_operator.hpp"

namespace duckdb {
//! PhysicalCrossProduct represents a cross product between two tables
class PhysicalCrossProduct : public PhysicalOperator {
public:
	PhysicalCrossProduct(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right);

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalCrossProductOperatorState : public PhysicalOperatorState {
public:
	PhysicalCrossProductOperatorState(PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(left), left_position(0) {
		assert(left && right);
	}

	index_t left_position;
	index_t right_position;
	ChunkCollection right_data;
};
} // namespace duckdb
