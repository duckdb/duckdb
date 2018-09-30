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

#include <unordered_set>
#include <vector>

#include "catalog/catalog.hpp"

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "parser/expression.hpp"
#include "parser/statement/select_statement.hpp"

#include "planner/logical_operator_visitor.hpp"

namespace duckdb {
//! LogicalOperator is the base class of the logical operators present in the
//! logical query tree
class LogicalOperator : public Printable {
  public:
	LogicalOperator(LogicalOperatorType type) : type(type) {}

	LogicalOperator(LogicalOperatorType type,
	                std::vector<std::unique_ptr<Expression>> expressions)
	    : type(type), expressions(std::move(expressions)) {}

	LogicalOperatorType GetOperatorType() { return type; }

	virtual std::string ParamsToString() const {
		std::string result = "";
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

	void AddChild(std::unique_ptr<LogicalOperator> child) {
		referenced_tables.insert(child->referenced_tables.begin(),
		                         child->referenced_tables.end());
		children.push_back(move(child));
	}

	//! The type of the logical operator
	LogicalOperatorType type;
	//! The set of tables that is accessible from this operator
	std::unordered_set<size_t> referenced_tables;
	//! The set of children of the operator
	std::vector<std::unique_ptr<LogicalOperator>> children;
	//! The set of expressions contained within the operator, if any
	std::vector<std::unique_ptr<Expression>> expressions;

	virtual size_t ExpressionCount() { return expressions.size(); }

	virtual Expression *GetExpression(size_t index) {
		if (index >= ExpressionCount()) {
			throw OutOfRangeException(
			    "GetExpression(): Expression index out of range!");
		}
		return expressions[index].get();
	}

	virtual void SetExpression(size_t index, std::unique_ptr<Expression> expr) {
		if (index >= ExpressionCount()) {
			throw OutOfRangeException(
			    "SetExpression(): Expression index out of range!");
		}
		expressions[index] = std::move(expr);
	}
};
} // namespace duckdb
