#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {
class PhysicalUnion : public PhysicalOperator {
  public:
	PhysicalUnion(std::unique_ptr<PhysicalOperator> top,
	              std::unique_ptr<PhysicalOperator> bottom);

	std::vector<TypeId> GetTypes() override;
	virtual void GetChunk(ClientContext &context, DataChunk &chunk,
	                      PhysicalOperatorState *state) override;
	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;
};

class PhysicalUnionOperatorState : public PhysicalOperatorState {
  public:
	PhysicalUnionOperatorState(ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(nullptr, parent_executor), top_done(false) {}
	std::unique_ptr<PhysicalOperatorState> top_state;
	std::unique_ptr<PhysicalOperatorState> bottom_state;
	bool top_done = false;
};

}; // namespace duckdb
