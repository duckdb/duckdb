
#pragma once

#include "parser/expression/tableref_expression.hpp"

namespace duckdb {
class CrossProductExpression : public TableRefExpression {
  public:
	CrossProductExpression()
	    : TableRefExpression(TableReferenceType::CROSS_PRODUCT) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }
};
} // namespace duckdb