
#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
class GroupRefExpression : public AbstractExpression {
  public:
	GroupRefExpression(TypeId return_type, size_t group_index)
	    : AbstractExpression(ExpressionType::GROUP_REF, return_type),
	      group_index(group_index) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

	size_t group_index;
};
}
