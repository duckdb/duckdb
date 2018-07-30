

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalFilter : public PhysicalOperator {
  public:
	PhysicalFilter(std::vector<std::unique_ptr<AbstractExpression>> select_list)
	    : PhysicalOperator(PhysicalOperatorType::FILTER),
	      expressions(std::move(select_list)) {}

	virtual void InitializeChunk(DataChunk &chunk) override;
	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	std::vector<std::unique_ptr<AbstractExpression>> expressions;
};
} // namespace duckdb
