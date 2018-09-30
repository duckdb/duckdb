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

#include "parser/expression.hpp"

namespace duckdb {
//! Represents a reference to one of the GROUP BY columns
class GroupRefExpression : public Expression {
  public:
	GroupRefExpression(TypeId return_type, size_t group_index)
	    : Expression(ExpressionType::GROUP_REF, return_type),
	      group_index(group_index) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	virtual bool Equals(const Expression *other_) override {
		if (!Expression::Equals(other_)) {
			return false;
		}
		auto other = reinterpret_cast<const GroupRefExpression *>(other_);
		if (!other) {
			return false;
		}
		return group_index == other->group_index;
	}

	virtual bool IsScalar() override { return false; }

	//! The index of the group this expression is referencing
	size_t group_index;
};
} // namespace duckdb
