#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {

class CaseClause {
  public:
	CaseClause(std::unique_ptr<AbstractExpression> when,
	           std::unique_ptr<AbstractExpression> result)
	    : when(move(when)), result(move(result)) {}

	std::unique_ptr<AbstractExpression> when;
	std::unique_ptr<AbstractExpression> result;
};

class CaseExpression : public AbstractExpression {
  public:
	CaseExpression(
	    std::unique_ptr<std::vector<std::unique_ptr<duckdb::CaseClause>>>
	        clauses,
	    std::unique_ptr<AbstractExpression> defresult)
	    : AbstractExpression(ExpressionType::OPERATOR_CASE_EXPR) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

	std::vector<std::unique_ptr<CaseClause>> clauses;
	std::unique_ptr<AbstractExpression> defresult;
};
} // namespace duckdb
