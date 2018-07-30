
#pragma once

#include "parser/expression/tableref_expression.hpp"
#include "parser/statement/select_statement.hpp"

namespace duckdb {
class SubqueryExpression : public TableRefExpression {
  public:
	SubqueryExpression() : TableRefExpression(TableReferenceType::SUBQUERY) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

	std::unique_ptr<SelectStatement> subquery;
};
} // namespace duckdb
