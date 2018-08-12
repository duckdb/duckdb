//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/physical_cross_product.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {
//! PhysicalCrossProduct represents a cross product between two tables
class PhysicalCrossProduct : public PhysicalOperator {
  public:
	PhysicalCrossProduct(std::unique_ptr<PhysicalOperator> left,
	                     std::unique_ptr<PhysicalOperator> right);

	std::vector<TypeId> GetTypes() override;
	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;
};

class PhysicalCrossProductOperatorState : public PhysicalOperatorState {
  public:
	PhysicalCrossProductOperatorState(PhysicalOperator *left,
	                                  PhysicalOperator *right,
	                                  ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(left, parent_executor), left_position(0),
	      right_chunk(0) {
		assert(left && right);
		sel_vector = std::unique_ptr<sel_t[]>(new sel_t[STANDARD_VECTOR_SIZE]);
		for (size_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			sel_vector[i] = 0;
		}
	}

	std::unique_ptr<sel_t[]> sel_vector;
	size_t left_position;
	size_t right_chunk;
	std::vector<std::unique_ptr<DataChunk>> right_chunks;
};
} // namespace duckdb
