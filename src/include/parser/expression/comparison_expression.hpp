//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/comparison_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
//! Represents a boolean comparison (e.g. =, >=, <>). Always returns a boolean
//! and has two children.
class ComparisonExpression : public AbstractExpression {
  public:
	ComparisonExpression(ExpressionType type,
	                     std::unique_ptr<AbstractExpression> left,
	                     std::unique_ptr<AbstractExpression> right)
	    : AbstractExpression(type, TypeId::BOOLEAN, std::move(left),
	                         std::move(right)) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
};
} // namespace duckdb
