//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_filter.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalFilter represents a filter operation (e.g. WHERE or HAVING clause)
class LogicalFilter : public LogicalOperator {
  public:
	LogicalFilter(std::unique_ptr<AbstractExpression> expression);

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	//! The set of expressions that have to be satisfied for a tuple to pass the
	//! filter
	std::vector<std::unique_ptr<AbstractExpression>> expressions;

	virtual std::string ParamsToString() const override {
		std::string result = "";
		// TODO: generify this somehow?
		if (expressions.size() > 0) {
			result += "[";
			for (size_t i = 0; i < expressions.size(); i++) {
				auto &child = expressions[i];
				result += child->ToString();
				if (i < expressions.size() - 1) {
					result += ", ";
				}
			}
			result += "]";
		}

		return result;
	}

  private:
	void SplitPredicates(std::unique_ptr<AbstractExpression> expression);
};

} // namespace duckdb
