

#pragma once

#include "execution/physical_operator.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

class PhysicalLimit : public PhysicalOperator {
  public:
	PhysicalLimit(size_t limit, size_t offset)
	    : PhysicalOperator(PhysicalOperatorType::LIMIT), limit(limit),
	      offset(offset) {}

	size_t limit;
	size_t offset;

	virtual void InitializeChunk(DataChunk &chunk) override;
	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalLimitOperatorState : public PhysicalOperatorState {
  public:
	PhysicalLimitOperatorState(PhysicalOperator *child,
	                           size_t current_offset = 0)
	    : PhysicalOperatorState(child), current_offset(current_offset) {}

	size_t current_offset;
};
} // namespace duckdb
