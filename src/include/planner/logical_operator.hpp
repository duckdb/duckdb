
#pragma once

#include <vector>

#include "catalog/catalog.hpp"

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "parser/expression/abstract_expression.hpp"
#include "parser/statement/select_statement.hpp"

#include "planner/logical_visitor.hpp"

namespace duckdb {
class LogicalOperator : public Printable {
  public:
	LogicalOperator(LogicalOperatorType type) : type(type) {}

	LogicalOperatorType GetOperatorType() { return type; }

	virtual std::string ToString() const override {
		std::string result = LogicalOperatorToString(type);
		if (children.size() > 0) {
			result += " ( ";
			for (auto &child : children) {
				result += child->ToString();
			}
			result += " )";
		}
		return result;
	}

	virtual void Accept(LogicalOperatorVisitor *) = 0;
	virtual void AcceptChildren(LogicalOperatorVisitor *v) {
		for (auto &child : children) {
			child->Accept(v);
		}
	}

	LogicalOperatorType type;
	std::vector<std::unique_ptr<LogicalOperator>> children;
};
}
