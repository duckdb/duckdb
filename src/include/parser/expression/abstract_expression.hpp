
#pragma once

#include <memory>
#include <vector>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "parser/sql_node_visitor.hpp"

namespace duckdb {
class AggregateExpression;

class AbstractExpression : public Printable {
  public:
	AbstractExpression(ExpressionType type) : type(type), parent(nullptr) {}
	AbstractExpression(ExpressionType type, TypeId return_type)
	    : type(type), return_type(return_type), parent(nullptr) {}
	AbstractExpression(ExpressionType type, TypeId return_type,
	                   std::unique_ptr<AbstractExpression> left,
	                   std::unique_ptr<AbstractExpression> right)
	    : type(type), return_type(return_type), parent(nullptr) {
		// Order of these is important!
		if (left != nullptr)
			AddChild(std::move(left));
		if (right != nullptr)
			AddChild(std::move(right));
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

	void AddChild(std::unique_ptr<AbstractExpression> child) {
		child->parent = this;
		children.push_back(std::move(child));
	}

	virtual void GetAggregates(std::vector<AggregateExpression *> &expressions);
	virtual bool IsAggregate();

	ExpressionType GetExpressionType() { return type; }

	ExpressionType type;
	TypeId return_type = TypeId::INVALID;

	std::string alias;

	AbstractExpression *parent;
	std::vector<std::unique_ptr<AbstractExpression>> children;
};
} // namespace duckdb
