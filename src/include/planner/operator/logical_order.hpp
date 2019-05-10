//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/bound_query_node.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalOrder represents an ORDER BY clause, sorting the data
class LogicalOrder : public LogicalOperator {
public:
	LogicalOrder(vector<BoundOrderByNode> orders)
	    : LogicalOperator(LogicalOperatorType::ORDER_BY), orders(move(orders)) {
	}

	vector<BoundOrderByNode> orders;

	uint64_t ExpressionCount() override;
	Expression *GetExpression(uint64_t index) override;
	void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                       uint64_t index) override;

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
