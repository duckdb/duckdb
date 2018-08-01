
#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
class CastExpression : public AbstractExpression {
  public:
	CastExpression(TypeId target, std::unique_ptr<AbstractExpression> child)
	    : AbstractExpression(ExpressionType::OPERATOR_CAST, target,
	                         std::move(child)) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }
};
} // namespace duckdb
