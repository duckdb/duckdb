
#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalOrder : public PhysicalOperator {
  public:
	PhysicalOrder(OrderByDescription description)
	    : PhysicalOperator(PhysicalOperatorType::ORDER_BY),
	      description(std::move(description)) {}

	virtual void InitializeChunk(DataChunk &chunk) override;
	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	OrderByDescription description;
};

class PhysicalOrderOperatorState : public PhysicalOperatorState {
  public:
	PhysicalOrderOperatorState(PhysicalOperator *child)
	    : PhysicalOperatorState(child), position(0) {}

	size_t position;
	DataChunk sorted_data;
};
} // namespace duckdb
