
#pragma once

#include "common/types/value.hpp"
#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
class ConstantExpression : public AbstractExpression {
  public:
	ConstantExpression()
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT), value() {}
	ConstantExpression(std::string val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT, TypeId::VARCHAR),
	      value(val) {}
	ConstantExpression(int32_t val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT, TypeId::INTEGER),
	      value(val) {}
	ConstantExpression(double val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT, TypeId::DECIMAL),
	      value(val) {}
	ConstantExpression(const Value &val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT, val.type),
	      value(val) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

	Value value;
};
}
