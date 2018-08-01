//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/groupref_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
//! Represents a reference to one of the GROUP BY columns
class GroupRefExpression : public AbstractExpression {
  public:
	GroupRefExpression(TypeId return_type, size_t group_index)
	    : AbstractExpression(ExpressionType::GROUP_REF, return_type),
	      group_index(group_index) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

	//! The index of the group this expression is referencing
	size_t group_index;
};
}
