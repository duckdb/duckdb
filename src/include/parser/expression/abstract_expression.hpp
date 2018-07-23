
#pragma once

#include <memory>
#include <vector>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "parser/sql_node_visitor.hpp"

namespace duckdb {
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

	virtual void Accept(SQLNodeVisitor *) = 0;
	virtual void AcceptChildren(SQLNodeVisitor *v) {
		for (auto &child : children) {
			child->Accept(v);
		}
	}

	virtual void ResolveType() {
		for (auto &child : children) {
			child->ResolveType();
		}
	}

	virtual bool IsAggregate();

	ExpressionType GetExpressionType() { return type; }

	ExpressionType type;
	TypeId return_type = TypeId::INVALID;

	std::string alias;

	std::vector<std::unique_ptr<AbstractExpression>> children;
};
}
