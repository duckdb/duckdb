//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/star_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {

//! Represents a STAR expression in SELECT clause
class StarExpression : public Expression {
  public:
	//! STAR expression in SELECT clause
	StarExpression() : Expression(ExpressionType::STAR) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	virtual std::string ToString() const override { return "*"; }

	virtual bool IsScalar() override { return false; }
};
} // namespace duckdb
