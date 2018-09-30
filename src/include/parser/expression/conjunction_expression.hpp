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

#include "parser/expression.hpp"

namespace duckdb {
//! Represents a conjunction (AND/OR)
class ConjunctionExpression : public Expression {
  public:
	ConjunctionExpression(ExpressionType type, std::unique_ptr<Expression> left,
	                      std::unique_ptr<Expression> right)
	    : Expression(type, TypeId::BOOLEAN, std::move(left), std::move(right)) {
	}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
};
} // namespace duckdb
