
#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
class TableRefExpression : public AbstractExpression {
  public:
	TableRefExpression(TableReferenceType ref_type)
	    : AbstractExpression(ExpressionType::TABLE_REF), ref_type(ref_type) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	TableReferenceType ref_type;
};
} // namespace duckdb
