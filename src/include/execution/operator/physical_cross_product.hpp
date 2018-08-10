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
//! PhyisicalLimit represents the LIMIT operator
class PhysicalCrossProduct : public PhysicalOperator {
  public:
	PhysicalCrossProduct(std::unique_ptr<PhysicalOperator> left,
	                     std::unique_ptr<PhysicalOperator> right);

	std::vector<TypeId> GetTypes() override;
	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalCrossProductOperatorState : public PhysicalOperatorState {
  public:
	PhysicalCrossProductOperatorState(PhysicalOperator *left,
	                                  PhysicalOperator *right)
	    : PhysicalOperatorState(left), left_position(0), right_chunk(0) {
		assert(left && right);
	}

	size_t left_position;
	size_t right_chunk;
	std::vector<std::unique_ptr<DataChunk>> right_chunks;
};
} // namespace duckdb
