
#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalProjection : public PhysicalOperator {
  public:
	PhysicalProjection(
	    std::vector<std::unique_ptr<AbstractExpression>> select_list)
	    : PhysicalOperator(PhysicalOperatorType::PROJECTION),
	      select_list(move(select_list)) {}

	virtual void InitializeChunk(DataChunk &chunk) override;
	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	std::vector<std::unique_ptr<AbstractExpression>> select_list;
};

class PhysicalProjectionOperatorState : public PhysicalOperatorState {
  public:
	PhysicalProjectionOperatorState(PhysicalOperator *child)
	    : PhysicalOperatorState(child), executed(false) {}

	bool executed;
};
}
