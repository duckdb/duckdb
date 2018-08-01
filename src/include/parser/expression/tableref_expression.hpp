//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/tableref_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
//! Represents a generic expression that returns a table.
class TableRefExpression : public AbstractExpression {
  public:
	TableRefExpression(TableReferenceType ref_type)
	    : AbstractExpression(ExpressionType::TABLE_REF), ref_type(ref_type) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	TableReferenceType ref_type;
};
} // namespace duckdb
