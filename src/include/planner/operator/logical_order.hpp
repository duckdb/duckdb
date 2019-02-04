//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/query_node/select_node.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalOrder represents an ORDER BY clause, sorting the data
class LogicalOrder : public LogicalOperator {
public:
	LogicalOrder(OrderByDescription description)
	    : LogicalOperator(LogicalOperatorType::ORDER_BY), description(std::move(description)) {
	}

	vector<string> GetNames() override {
		return children[0]->GetNames();
	}

	OrderByDescription description;

	size_t ExpressionCount() override;
	Expression *GetExpression(size_t index) override;
	void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback, size_t index) override;
protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
