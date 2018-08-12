//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/logical_operator.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/catalog.hpp"

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "parser/expression/abstract_expression.hpp"
#include "parser/statement/select_statement.hpp"

#include "planner/logical_operator_visitor.hpp"

namespace duckdb {
//! LogicalOperator is the base class of the logical operators present in the
//! logical query tree
class LogicalOperator : public Printable {
  public:
	LogicalOperator(LogicalOperatorType type) : type(type) {}

	LogicalOperatorType GetOperatorType() { return type; }

	virtual std::string ParamsToString() const { return std::string(); }

	virtual std::string ToString() const override {
		std::string result = LogicalOperatorToString(type);
		result += ParamsToString();
		if (children.size() > 0) {
			result += "(";
			for (size_t i = 0; i < children.size(); i++) {
				auto &child = children[i];
				result += child->ToString();
				if (i < children.size() - 1) {
					result += ", ";
				}
			}
			result += ")";
		}

		return result;
	}

	virtual void Accept(LogicalOperatorVisitor *) = 0;
	virtual void AcceptChildren(LogicalOperatorVisitor *v) {
		for (auto &child : children) {
			child->Accept(v);
		}
	}

	//! The type of the logical operator
	LogicalOperatorType type;
	//! The set of children of the operator
	std::vector<std::unique_ptr<LogicalOperator>> children;
};
} // namespace duckdb
