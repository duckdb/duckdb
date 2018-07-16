
#pragma once

#include "parser/expression/abstract_expression.hpp"
#include "type/value.hpp"

class ConstantExpression : public AbstractExpression {
  public:
	ConstantExpression()
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT), value() {}
	ConstantExpression(std::string val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT), value(val) {}
	ConstantExpression(int32_t val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT), value(val) {}
	ConstantExpression(double val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT), value(val) {}

	virtual std::string ToString() const { return std::string(); }

  private:
	Value value;
};
