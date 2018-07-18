
#pragma once

#include "parser/expression/tableref_expression.hpp"

namespace duckdb {
class JoinExpression : public TableRefExpression {
  public:
	JoinExpression() : TableRefExpression(TableReferenceType::JOIN) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

	std::unique_ptr<AbstractExpression> left;
	std::unique_ptr<AbstractExpression> right;
	std::unique_ptr<AbstractExpression> condition;
	JoinType type;
};
}