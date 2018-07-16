
#pragma once

#include "common/internal_types.hpp"
#include "common/printable.hpp"

class AbstractExpression : public Printable {
  public:
	AbstractExpression(ExpressionType type) : type(type) {}
	AbstractExpression(ExpressionType type, TypeId return_type)
	    : type(type), return_type(return_type) {}
	AbstractExpression(ExpressionType type, TypeId return_type,
	                   std::unique_ptr<AbstractExpression> left,
	                   std::unique_ptr<AbstractExpression> right)
	    : type(type), return_type(return_type) {
		// Order of these is important!
		if (left != nullptr)
			children.push_back(move(left));
		if (right != nullptr)
			children.push_back(move(right));
	}

	ExpressionType GetExpressionType() { return type; }

  protected:
	ExpressionType type;
	TypeId return_type = TypeId::INVALID;

	std::vector<std::unique_ptr<AbstractExpression>> children;
};
