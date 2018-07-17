
#pragma once

#include "common/value.hpp"
#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
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
	ConstantExpression(const Value &val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT), value(val) {}

	virtual std::string ToString() const { return std::string(); }

	Value value;
};
}