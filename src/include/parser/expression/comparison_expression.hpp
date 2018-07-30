
#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
class ComparisonExpression : public AbstractExpression {
  public:
	ComparisonExpression(ExpressionType type,
	                     std::unique_ptr<AbstractExpression> left,
	                     std::unique_ptr<AbstractExpression> right)
	    : AbstractExpression(type, TypeId::BOOLEAN, std::move(left),
	                         std::move(right)) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }
};
} // namespace duckdb
