//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/helper/physical_limit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! PhyisicalLimit represents the LIMIT operator
class PhysicalLimit : public PhysicalOperator {
public:
	PhysicalLimit(LogicalOperator &op, uint64_t limit, uint64_t offset)
	    : PhysicalOperator(PhysicalOperatorType::LIMIT, op.types), limit(limit), offset(offset) {
	}

	uint64_t limit;
	uint64_t offset;

	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalLimitOperatorState : public PhysicalOperatorState {
public:
	PhysicalLimitOperatorState(PhysicalOperator *child, uint64_t current_offset = 0)
	    : PhysicalOperatorState(child), current_offset(current_offset) {
	}

	uint64_t current_offset;
};
} // namespace duckdb
