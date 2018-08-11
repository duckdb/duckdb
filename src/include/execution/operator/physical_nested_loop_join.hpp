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

#include "execution/operator/physical_cross_product.hpp"
#include "execution/physical_operator.hpp"

namespace duckdb {
//! PhysicalNestedLoopJoin represents a nested loop join between two tables
class PhysicalNestedLoopJoin : public PhysicalOperator {
  public:
	PhysicalNestedLoopJoin(std::unique_ptr<PhysicalOperator> left,
	                       std::unique_ptr<PhysicalOperator> right,
	                       std::unique_ptr<AbstractExpression> cond,
	                       JoinType join_type);

	std::vector<TypeId> GetTypes() override;
	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	PhysicalCrossProduct cross_product;
	std::unique_ptr<AbstractExpression> condition;
	JoinType type;
};
} // namespace duckdb
