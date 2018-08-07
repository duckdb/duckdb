//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/conjunction_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
//! Represents a conjunction (AND/OR)
class ConjunctionExpression : public AbstractExpression {
  public:
	ConjunctionExpression(ExpressionType type,
	                      std::unique_ptr<AbstractExpression> left,
	                      std::unique_ptr<AbstractExpression> right)
	    : AbstractExpression(type, TypeId::BOOLEAN, std::move(left),
	                         std::move(right)) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
};
} // namespace duckdb
