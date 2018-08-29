//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/physical_projection.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalProjection : public PhysicalOperator {
  public:
	PhysicalProjection(
	    std::vector<std::unique_ptr<AbstractExpression>> select_list)
	    : PhysicalOperator(PhysicalOperatorType::PROJECTION),
	      select_list(move(select_list)) {}

	std::vector<TypeId> GetTypes() override;
	virtual void GetChunk(ClientContext &context, DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	std::vector<std::unique_ptr<AbstractExpression>> select_list;
};

} // namespace duckdb
