//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_aggregate.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalAggregate : public LogicalOperator {
  public:
	LogicalAggregate(
	    std::vector<std::unique_ptr<AbstractExpression>> select_list)
	    : LogicalOperator(LogicalOperatorType::AGGREGATE_AND_GROUP_BY,
	                      std::move(select_list)) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	//! The set of groups (optional).
	std::vector<std::unique_ptr<AbstractExpression>> groups;

	virtual size_t ExpressionCount() override {
		return expressions.size() + groups.size();
	}

	virtual AbstractExpression *GetExpression(size_t index) override {
		if (index >= ExpressionCount()) {
			throw OutOfRangeException();
		}
		if (index >= expressions.size()) {
			return groups[index - expressions.size()].get();
		}
		return expressions[index].get();
	}

	virtual void
	SetExpression(size_t index,
	              std::unique_ptr<AbstractExpression> expr) override {
		if (index >= ExpressionCount()) {
			throw OutOfRangeException();
		}
		if (index >= expressions.size()) {
			groups[index - expressions.size()] = std::move(expr);
		} else {
			expressions[index] = std::move(expr);
		}
	}

	virtual std::string ParamsToString() const override {
		std::string result = LogicalOperator::ParamsToString();
		if (groups.size() > 0) {
			result += "[";
			for (size_t i = 0; i < groups.size(); i++) {
				auto &child = groups[i];
				result += child->ToString();
				if (i < groups.size() - 1) {
					result += ", ";
				}
			}
			result += "]";
		}

		return result;
	}
};
} // namespace duckdb
