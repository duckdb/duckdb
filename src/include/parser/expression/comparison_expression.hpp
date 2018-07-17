
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

	virtual std::string ToString() const { return std::string(); }
};
}
