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

	virtual bool Equals(const AbstractExpression *other_) override {
		if (!AbstractExpression::Equals(other_)) {
			return false;
		}
		auto other = reinterpret_cast<const GroupRefExpression *>(other_);
		if (!other) {
			return false;
		}
		return group_index == other->group_index;
	}

	//! The index of the group this expression is referencing
	size_t group_index;
};
} // namespace duckdb
