//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_comparison_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

//! PhysicalJoin represents the base class of the join operators
class PhysicalComparisonJoin : public PhysicalJoin {
public:
	PhysicalComparisonJoin(LogicalOperator &op, PhysicalOperatorType type, vector<JoinCondition> cond,
	                       JoinType join_type);

	vector<JoinCondition> conditions;

public:
	string ExtraRenderInformation() const override;
};

class PhysicalComparisonJoinState : public PhysicalOperatorState {
public:
	PhysicalComparisonJoinState(PhysicalOperator *left, PhysicalOperator *right, vector<JoinCondition> &conditions)
	    : PhysicalOperatorState(left) {
		assert(left && right);
		for (auto &cond : conditions) {
			lhs_executor.AddExpression(*cond.left);
			rhs_executor.AddExpression(*cond.right);
		}
	}

	ExpressionExecutor lhs_executor;
	ExpressionExecutor rhs_executor;
};

} // namespace duckdb
