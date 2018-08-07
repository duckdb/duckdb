//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/crossproduct_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/tableref_expression.hpp"

namespace duckdb {
//! Represents a cross product
class CrossProductExpression : public TableRefExpression {
  public:
	CrossProductExpression()
	    : TableRefExpression(TableReferenceType::CROSS_PRODUCT) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
};
} // namespace duckdb
