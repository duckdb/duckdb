#pragma once

#include "parser/abstract_expression.hpp"

namespace duckdb {

class CaseExpression : public AbstractExpression {
  public:
	// this expression has 3 children, the test and the result if the test
	// evaluates to 1 and the result if it does not
	CaseExpression() : AbstractExpression(ExpressionType::OPERATOR_CASE_EXPR) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	virtual void ResolveType() override {
		AbstractExpression::ResolveType();
		return_type =
		    std::max(children[1]->return_type, children[2]->return_type);
	}
};
} // namespace duckdb
