//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_projection.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalProjection represents the projection list in a SELECT clause
class LogicalProjection : public LogicalOperator {
  public:
	LogicalProjection(
	    std::vector<std::unique_ptr<AbstractExpression>> select_list)
	    : LogicalOperator(LogicalOperatorType::PROJECTION),
	      select_list(move(select_list)) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	virtual std::string ParamsToString() const override {
		std::string result = "";
		// TODO: generify this somehow?
		if (select_list.size() > 0) {
			result += "[";
			for (size_t i = 0; i < select_list.size(); i++) {
				auto &child = select_list[i];
				result += child->ToString();
				if (i < select_list.size() - 1) {
					result += ", ";
				}
			}
			result += "]";
		}

		return result;
	}

	//! The projection list
	std::vector<std::unique_ptr<AbstractExpression>> select_list;
};
} // namespace duckdb
