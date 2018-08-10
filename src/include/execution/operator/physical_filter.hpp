//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/physical_filter.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalFilter represents a filter operator. It removes non-matching tupels
//! from the result. Note that it does not physically change the data, it only
//! adds a selection vector to the chunk.
class PhysicalFilter : public PhysicalOperator {
  public:
	PhysicalFilter(std::vector<std::unique_ptr<AbstractExpression>> select_list)
	    : PhysicalOperator(PhysicalOperatorType::FILTER),
	      expressions(std::move(select_list)) {}

	std::vector<TypeId> GetTypes() override;
	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent) override;

	std::vector<std::unique_ptr<AbstractExpression>> expressions;
};
} // namespace duckdb
