
#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
class OperatorExpression : public AbstractExpression {
  public:
	OperatorExpression(ExpressionType type, TypeId type_id = TypeId::INVALID)
	    : AbstractExpression(type, type_id) {}
	OperatorExpression(ExpressionType type, TypeId type_id,
	                   std::unique_ptr<AbstractExpression> left,
	                   std::unique_ptr<AbstractExpression> right)
	    : AbstractExpression(type, type_id, std::move(left), std::move(right)) {
	}

	virtual void ResolveType() override {
		AbstractExpression::ResolveType();
		// logical operators return a bool
		if (type == ExpressionType::OPERATOR_NOT ||
		    type == ExpressionType::OPERATOR_IS_NULL ||
		    type == ExpressionType::OPERATOR_IS_NOT_NULL ||
		    type == ExpressionType::OPERATOR_EXISTS) {
			return_type = TypeId::BOOLEAN;
			return;
		}
		// other operators return the highest type of the children
		return_type =
		    std::max(children[0]->return_type, children[1]->return_type);
	}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }
};
}