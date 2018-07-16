
#pragma once

#include "parser/expression/abstract_expression.hpp"

class OperatorExpression : public AbstractExpression {
  public:
	OperatorExpression(ExpressionType type, TypeId type_id)
	    : AbstractExpression(type, type_id) {}
	OperatorExpression(ExpressionType type, TypeId type_id,
	                   std::unique_ptr<AbstractExpression> left,
	                   std::unique_ptr<AbstractExpression> right)
	    : AbstractExpression(type, type_id, std::move(left), std::move(right)) {
	}

	virtual std::string ToString() const { return std::string(); }
};
